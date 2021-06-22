from itertools import chain
from operator import contains
from pathlib import Path

from frictionless import Resource
from glom import glom, Iter, T
from glom.matching import MatchError
import numpy as np
import pandas as pd
import pytest

from friendly_data.converters import _schema, _source_type
from friendly_data.dpkg import create_pkg
from friendly_data.dpkg import fullpath
from friendly_data.dpkg import index_levels
from friendly_data.dpkg import idxpath_from_pkgpath
from friendly_data.dpkg import pkg_from_files
from friendly_data.dpkg import pkg_from_index
from friendly_data.dpkg import read_pkg
from friendly_data.dpkg import res_from_entry
from friendly_data.dpkg import pkgindex
from friendly_data.dpkg import update_pkg
from friendly_data.dpkg import write_pkg
from friendly_data.helpers import match, noop_map, select, is_windows
from friendly_data.io import relpaths
from friendly_data.metatools import get_license
import friendly_data_registry as registry

from .conftest import expected_schema, assert_log


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


def test_source_type_heuristics():
    with pytest.raises(ValueError):
        _source_type("/path/to/non-existent-file.ext")


def test_pkg_creation():
    pkgdir = Path("testing/files/random")
    pkg_meta = {"name": "test", "licenses": get_license("CC0-1.0")}
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
    with pytest.raises(FileNotFoundError, match=f"{pkg_json}: not found"):
        read_pkg(dest)
    with pytest.raises(FileNotFoundError, match=f"{pkg_json}: not found"):
        read_pkg(pkg_json)


def test_pkg_update(rnd_pkg):
    # update a single column in a dataset
    resource_name = "sample-ok-1"
    update = {"time": {"name": "time", "type": "string", "format": "default"}}
    assert update_pkg(rnd_pkg, resource_name, update)
    res, *_ = glom(
        rnd_pkg,
        (
            "resources",
            [select(T["name"], equal_to=resource_name)],
            "0.schema.fields",
            [select(T["name"], equal_to="time")],
        ),
    )
    assert update["time"] == res

    # update multiple columns in a dataset
    update = {
        "time": {"name": "time", "type": "datetime", "format": "default"},
        "QWE": {"name": "QWE", "type": "string", "format": "default"},
    }
    assert update_pkg(rnd_pkg, resource_name, update)
    res = glom(
        rnd_pkg,
        (
            "resources",
            [select(T["name"], equal_to=resource_name)],
            "0.schema.fields",
            [select(T["name"], one_of=update.keys())],
        ),
    )
    assert list(update.values()) == res

    # update missing values, index columns
    resource_name = "sample-ok-2"
    update = {"primaryKey": ["lvl", "TRE", "IUY"]}
    assert update_pkg(rnd_pkg, resource_name, update, fields=False)
    res = glom(
        rnd_pkg,
        (
            "resources",
            [select(T["name"], equal_to=resource_name)],
            "0.schema.primaryKey",
        ),
    )
    assert update["primaryKey"] == res

    # FIXME: test assertions inside update_pkg


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


def test_res_from_entry():
    pkgdir = Path("testing/files/mini-ex")
    idxfile = pkgindex.from_file(pkgdir / "index.yaml")
    for entry in idxfile.records(["path", "idxcols", "alias"]):
        res = res_from_entry(entry, pkgdir)
        assert isinstance(res, Resource)
        assert glom(res, "schema.primaryKey") == entry["idxcols"]


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
    _, pkg, idx = pkg_from_files(pkg_meta, pkgdir / "index.json", files)
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
