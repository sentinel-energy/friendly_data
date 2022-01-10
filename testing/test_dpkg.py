from copy import deepcopy
from itertools import chain
from operator import contains
from pathlib import Path
from urllib.parse import urlparse

from frictionless import Resource
from glom import glom, Iter, T
from glom.matching import MatchError
import numpy as np  # noqa: F401
import pandas as pd
import pytest

from friendly_data.converters import _schema
from friendly_data.dpkg import create_pkg, resource_, set_idxcols
from friendly_data.dpkg import entry_from_res
from friendly_data.dpkg import fullpath
from friendly_data.dpkg import index_levels
from friendly_data.dpkg import idxpath_from_pkgpath
from friendly_data.dpkg import pkg_from_files
from friendly_data.dpkg import pkg_from_index
from friendly_data.dpkg import read_pkg
from friendly_data.dpkg import res_from_entry
from friendly_data.dpkg import pkgindex
from friendly_data.dpkg import write_pkg
from friendly_data.helpers import match, noop_map, is_windows
from friendly_data.io import dwim_file, relpaths
import friendly_data_registry as registry

from .conftest import expected_schema, escape_path, assert_log


@pytest.mark.skipif(not is_windows(), reason="only relevant for windows")
def test_ensure_posix(pkg_meta):
    pkgdir = Path("testing/files/mini-ex")
    files = chain(pkgdir.glob("inputs/*"), pkgdir.glob("outputs/*"))
    pkg = create_pkg(pkg_meta, relpaths(pkgdir, files), pkgdir)
    # NOTE: count windows path separators in the resource path, should be 0 as the
    # spec requires resource paths to be POSIX paths
    npathsep = glom(
        pkg, ("resources", Iter().map("path").map(T.count("\\")).all(), sum)
    )
    assert npathsep == 0


@pytest.mark.parametrize(
    "update",
    [
        {},
        {
            "timestep": {"name": "timestep", "type": "string"},
            "capacity_factor": {"name": "capacity_factor", "type": "float"},
        },
    ],
)
def test_resource_infer(update):
    pkgdir = Path("testing/files/mini-ex")
    spec = {"path": "outputs/capacity_factor.csv"}
    if update:
        spec["schema"] = {"fields": update}

    col_types = glom(
        resource_(spec, pkgdir).schema.fields,
        ([({1: "name", 2: "type"}, T.values(), tuple)], dict),
    )
    expected = {
        "carrier": "string",
        "region": "string",
        "technology": "string",
        "timestep": "datetime",
        "capacity_factor": "number",
    }
    expected.update((v["name"], v["type"]) for v in update.values())
    assert col_types == expected


def test_resource_err():
    with pytest.raises(ValueError, match="Incomplete resource.+\n.+'path' is missing"):
        resource_({})


@pytest.mark.parametrize(
    "basepath,path,opts,ncols",
    [
        ("testing/files/skip_test", "commented_dst.csv", {"skip": 1}, 4),
        ("testing/files/xls_sheet_test", "sheet_2.xlsx", {"sheet": 2}, 5),
        (
            "testing/files/sqlite",
            "sqlite:///testdb.db",
            {"name": "annual_cost_per_nameplate_capacity"},
            4,
        ),
    ],
)
def test_resource_opts(basepath, path, opts, ncols):
    for _dir in (basepath, ""):
        parsed = urlparse(path)
        if not parsed.scheme:
            _path = Path(_dir) / path
        elif parsed.scheme == "sqlite":
            # parsed.path has a leading slash
            if _dir:
                _path = f"{parsed.scheme}:///{_dir}{parsed.path}"
            else:
                _path = f"{parsed.scheme}://{parsed.path}"
        else:
            raise ValueError
        spec = {"path": _path, **opts}
        res = resource_(spec, "" if _dir else basepath)
        assert len(res.schema.fields) == ncols


def test_pkg_creation():
    pkgdir = Path("testing/files/random")
    pkg_meta = {"name": "test", "licenses": "CC0-1.0"}
    pkg = create_pkg(pkg_meta, relpaths(pkgdir, "data/sample-ok-?.csv"), pkgdir)
    for resource in pkg["resources"]:
        # match datapackage field type for datetime
        ts_cols = [
            field.name
            for field in resource.schema.fields
            if "datetime" in field["type"]
        ]
        expected = expected_schema(resource)
        expected.update((col, "datetime") for col in ts_cols)
        assert _schema(resource, noop_map()) == expected


def test_pkg_creation_skip_rows(pkg_meta):
    resources = [{"path": "commented_dst.csv", "skip": 1}]
    pkg = create_pkg(pkg_meta, resources, basepath="testing/files/skip_test")
    expected = ["timestep", "UK", "Ireland", "France"]
    assert glom(pkg, ("resources.0.schema.fields", Iter("name").all())) == expected


def test_pkg_read():
    pkgdir = Path("testing/files/random")
    dpkg_json = pkgdir / "datapackage.json"
    pkg = read_pkg(dpkg_json)
    assert all(fullpath(res).exists() for res in pkg["resources"])


def test_zippkg_read(rnd_pkg, tmp_path_factory):
    with tmp_path_factory.mktemp("ziptest-") as tmpdir:
        zipfile = tmpdir / "testpackage.zip"
        rnd_pkg.to_zip(f"{zipfile}")

        # unzip to current dir
        _ = read_pkg(zipfile)
        assert (zipfile.parent / "datapackage.json").exists()

        # unzip to different dir
        _ = read_pkg(zipfile, extract_dir=zipfile.parent / "foo")
        assert (zipfile.parent / "foo/datapackage.json").exists()


def test_pkg_read_error(tmp_pkgdir):
    # unsupported archive: tarball
    tarball = Path("/path/to/package.tar")
    with pytest.raises(ValueError, match=f".*{tarball.name}:.+"):
        read_pkg(tarball)

    _, dest = tmp_pkgdir
    pkg_json = dest / "datapackage.json"
    pkg_json.unlink()
    # NOTE: escape for Windows paths
    path = escape_path(pkg_json) if is_windows() else pkg_json
    with pytest.raises(FileNotFoundError, match=f"{path}: not found"):
        read_pkg(dest)
    with pytest.raises(FileNotFoundError, match=f"{path}: not found"):
        read_pkg(pkg_json)


@pytest.mark.parametrize("ext", [".yaml", ".yml", ".json"])
def test_pkgindex(ext):
    fpath = Path(f"testing/files/indices/index{ext}")
    idx = pkgindex.from_file(fpath)
    assert isinstance(idx, list)
    assert len(idx) == 3
    assert sum(map(len, idx.records(["path"]))) == len(idx)
    assert sum(map(len, idx.records(["path", "idxcols"]))) == len(idx) * 2
    assert sum(map(len, idx.records(["path", "idxcols", "skip"]))) == len(idx) * 3

    with pytest.raises(MatchError):
        idx.records(["path", "indexcols"])  # bad key: indexcols -> idxcols


def test_pkgindex_bad_file(caplog):
    fpath = Path("testing/files/indices/badindex.yaml")
    with pytest.raises(MatchError):
        # a bad key in the index file will trigger this error; aliases -> alias
        pkgindex.from_file(fpath)

    assert_log(caplog, "aliases: bad key in index file", "ERROR")


def test_pkgindex_errors(tmp_path):
    idxfile = tmp_path / "index.yaml"
    idxfile.touch()
    with pytest.raises(ValueError, match=f".*{idxfile.name}: bad index file"):
        pkgindex.from_file(idxfile)

    idxfile = idxfile.with_suffix(".txt")
    with pytest.raises(RuntimeError, match=f".*{idxfile.name}:.+"):
        pkgindex.from_file(idxfile)


@pytest.mark.parametrize("is_df", [False, True])
@pytest.mark.parametrize(
    "csvfile, idxcols",
    [
        ("inputs/cost_energy_cap.csv", ["cost", "region", "technology"]),
        ("inputs/cost_energy_cap.csv", ["cost", "region"]),  # specified columns only
        ("inputs/energy_eff.csv", ["region", "technology"]),
        ("inputs/description.csv", ["technology"]),
        (
            "outputs/capacity_factor.csv",
            ["carrier", "region", "technology", "timestep"],
        ),
        # specified columns only
        ("outputs/capacity_factor.csv", ["carrier", "technology", "timestep"]),
        ("outputs/resource_area.csv", ["region", "technology"]),
    ],
)
def test_index_levels(is_df, csvfile, idxcols):
    csvfile = Path("testing/files/mini-ex") / csvfile
    df = pd.read_csv(csvfile, index_col=idxcols)
    _, coldict = index_levels(df if is_df else csvfile, idxcols)
    assert all(map(contains, idxcols, coldict))
    # NOTE: metadata for categorical index columns isn't set as categorical if
    # they are not in the registry
    all_idxcols = glom(registry.getall(), ("idxcols", ["name"]))
    lvl_counts = {
        k: len(v)
        for k, v in zip(
            df.index.names,
            df.index.levels
            if isinstance(df.index, pd.MultiIndex)
            else [df.index.unique()],
        )
        if "timestep" not in k and k in all_idxcols  # category columns only
    }
    catcols = glom(
        coldict.values(),
        (
            Iter(match({"name": str, "constraints": {"enum": list}, str: str}))
            .map({1: "name", 2: ("constraints.enum", len)})
            .map(T.values())
            .all(),
            dict,
        ),
    )
    assert catcols == lvl_counts  # check all levels are found
    # TODO: aliased columns


@pytest.mark.parametrize("is_df", [False, True])
@pytest.mark.parametrize(
    "idxcols",
    [
        # 5 cases, partial (region) and completely set enum (in registry),
        # string (loc_*) or int (spore) enum not set (not in registry), and
        # datetime types (timestep) FIXME: doesn't cover completely set enum
        ("loc_from", "loc_to"),  # string enum not set
        ("spore", "region"),  # int enum not set, + partial
        ("spore", "loc_from", "loc_to"),  # int & string enum not set
        ("timestep", "spore", "loc_from", "loc_to"),  # datetime & enum not set
        ("timestep", "region", "spore", "loc_from", "loc_to"),  # all + partial
    ],
)
def test_index_levels2(is_df, idxcols):
    csvfile = Path("testing/files/index_levels/transmission_flows.csv")
    file_or_df = pd.read_csv(csvfile, index_col=idxcols) if is_df else csvfile
    file_or_df, coldict = index_levels(file_or_df, idxcols)
    assert all(col in coldict for col in idxcols)
    for col in idxcols:
        if col == "timestep":
            continue
        if registry.get(col, "idxcols"):
            nvals = len(file_or_df.index.get_level_values(col).unique())
            assert glom(coldict, (f"{col}.constraints.enum", len)) == nvals
        else:
            # when not in the registry, metadata will always be incomplete
            pass


def test_set_idxcols():
    pkgdir = Path("testing/files/iamc")
    res = set_idxcols("nameplate_capacity.csv", pkgdir)
    # scenario, unit, year
    assert len(glom(res, "schema.primaryKey")) == 3


def test_res_from_entry():
    pkgdir = Path("testing/files/mini-ex")
    idxfile = pkgindex.from_file(pkgdir / "index.yaml")
    for entry in idxfile.records(["path", "idxcols", "alias"]):
        res = res_from_entry(entry, pkgdir)
        assert isinstance(res, Resource)
        assert glom(res, "schema.primaryKey") == entry["idxcols"]


def test_res_from_entry_db():
    pkgdir = Path("testing/files/sqlite")
    idx = pkgindex.from_file(pkgdir / "index.yaml")
    entry = list(idx.records(["path", "name", "idxcols", "alias"]))[0]
    res = res_from_entry(entry, pkgdir)
    assert isinstance(res, Resource)
    assert glom(res, "schema.primaryKey") == entry["idxcols"]


def test_entry_from_res():
    pkgdir = Path("testing/files/alias_test")
    idx = dwim_file(pkgdir / "index.yaml")
    for entry in idx:
        expected = deepcopy(entry)
        res = res_from_entry(entry, pkgdir)
        assert entry_from_res(res) == expected


@pytest.mark.parametrize("idx_t", [".yaml", ".json"])
def test_pkg_from_index(idx_t, pkg_meta):
    idxpath = Path("testing/files/mini-ex/index").with_suffix(idx_t)
    pkgdir, pkg, _ = pkg_from_index(pkg_meta, idxpath)
    assert pkgdir == idxpath.parent
    assert len(pkg["resources"]) == 5  # number of datasets
    indices = glom(pkg, ("resources", Iter().map("schema.primaryKey").all()))
    assert len(indices) == 5
    # FIXME: not sure what else to check


def test_pkg_from_index_skip_rows(pkg_meta):
    _, pkg, _ = pkg_from_index(pkg_meta, "testing/files/skip_test/index.yaml")
    expected = ["timestep", "UK", "Ireland", "France"]
    assert glom(pkg, ("resources.0.schema.fields", Iter("name").all())) == expected


def test_pkg_from_index_aliased_cols(pkg_w_alias):
    ref = registry.get("region", "idxcols")
    col_schema, *_ = glom(
        pkg_w_alias,
        (
            "resources.0.schema.fields",
            Iter(match({"alias": str, str: object})).all(),
        ),
    )
    assert col_schema.pop("name") == "node"
    col_schema["name"] = col_schema.pop("alias")
    col_schema["constraints"]["enum"] = []  # registry doesn't include levels
    assert col_schema == ref


def test_pkg_from_files(pkg_meta, caplog):
    pkgdir = Path("testing/files/mini-ex")
    files = list(chain(pkgdir.glob("inputs/*"), pkgdir.glob("outputs/*")))
    # with index file
    _, pkg, idx = pkg_from_files(pkg_meta, pkgdir / "index.yaml", files)
    # files not in index: inheritance.csv, and loc_coordinates.csv
    assert len(pkg["resources"]) - len(idx) == 2

    # with directory containing index file
    _pkgdir, pkg, idx = pkg_from_files(pkg_meta, pkgdir, files)
    assert _pkgdir == pkgdir
    assert len(pkg["resources"]) - len(idx) == 2
    assert_log(caplog, "multiple indices:", "WARNING")


def test_pkg_from_files_no_index(pkg_meta, caplog):
    pkgdir = Path("testing/files/random")
    _, pkg, idx = pkg_from_files(pkg_meta, pkgdir, pkgdir.glob("data/*"))
    assert idx is None
    assert len(pkg["resources"]) == 3
    assert_log(caplog, "no index file", "WARNING")


def test_idxpath_from_pkgpath(tmp_path, caplog):
    idxpath = tmp_path / "index.json"
    assert idxpath_from_pkgpath(tmp_path) == ""
    assert_log(caplog, f"{tmp_path.name}: no index file found", "WARNING")

    idxpath.touch()
    assert idxpath_from_pkgpath(tmp_path) == idxpath

    idxpath.with_suffix(".yaml").touch()
    idxpath.with_suffix(".yml").touch()
    # NOTE: returns the lexicographically first match
    assert idxpath_from_pkgpath(tmp_path) == idxpath.with_suffix(".json")
    assert_log(caplog, "multiple indices:", "WARNING")


def test_write_pkg(pkg, tmp_path):
    res = write_pkg(pkg, tmp_path)
    assert len(res) == 1
    assert res[0].exists()

    assert not (tmp_path / "index.yaml").exists()


def test_write_pkg_idx(pkg, tmp_path):
    idx = pkgindex.from_file(f"{pkg.basepath}/index.yaml")

    res = write_pkg(pkg, tmp_path, idx=idx)
    assert len(res) == 2
    assert all([p.exists() for p in res])

    with pytest.raises(TypeError):
        write_pkg(pkg, tmp_path, idx)


@pytest.mark.skip(reason="not implemented")
def test_write_pkg_archive(pkg, tmp_path_factory):
    with tmp_path_factory.mktemp("pkgwrite-") as tmpdir:
        zipfile = tmpdir / "testpkg.zip"

        # save pkg as zip
        write_pkg(pkg, f"{zipfile}")
        assert zipfile.exists()

        # unsupported archive: tarball
        tarball = tmpdir / "testpkg.tar"
        with pytest.raises(ValueError, match=f".*{tarball.name}:.+"):
            write_pkg(pkg, f"{tarball}")
