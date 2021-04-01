from itertools import chain
from operator import contains
from pathlib import Path

from glom import glom, Iter, T
import numpy as np
import pytest

from friendly_data.converters import _schema, _source_type
from friendly_data.dpkg import create_pkg
from friendly_data.dpkg import fullpath
from friendly_data.dpkg import index_levels
from friendly_data.dpkg import idxpath_from_pkgpath
from friendly_data.dpkg import pkg_from_files
from friendly_data.dpkg import pkg_from_index
from friendly_data.dpkg import pkg_glossary
from friendly_data.dpkg import read_pkg
from friendly_data.dpkg import read_pkg_index
from friendly_data.dpkg import update_pkg
from friendly_data.dpkg import write_pkg
from friendly_data.helpers import match, select, is_windows
from friendly_data.io import relpaths
from friendly_data.metatools import get_license

from .conftest import expected_schema, noop_map


@pytest.mark.skipif(not is_windows(), reason="only relevant for windows")
def test_ensure_posix():
    pkgdir = Path("testing/files/mini-ex")
    meta = {
        "name": "foobarbaz",
        "title": "Foo Bar Baz",
        "licenses": "CC0-1.0",
        "keywords": ["foo", "bar", "baz"],
    }
    files = chain(pkgdir.glob("inputs/*"), pkgdir.glob("outputs/*"))
    pkg = create_pkg(meta, relpaths(pkgdir, files), pkgdir)
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


def test_pkg_creation_skip_rows():
    meta = {
        "name": "foobarbaz",
        "title": "Foo Bar Baz",
        "keywords": ["foo", "bar", "baz"],
        "license": ["CC0-1.0"],
    }
    resources = [{"path": "commented_dst.csv", "skip": 1}]
    pkg = create_pkg(meta, resources, basepath="testing/files/skip_test")
    expected = ["timesteps", "UK", "Ireland", "France"]
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


def test_pkg_read_error():
    # unsupported archive: tarball
    tarball = Path("/path/to/package.tar")
    with pytest.raises(ValueError, match=f".*{tarball.name}:.+"):
        read_pkg(tarball)


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
def test_read_pkg_index(ext):
    fpath = Path(f"testing/files/indices/index{ext}")
    idx = read_pkg_index(fpath)
    np.testing.assert_array_equal(idx.columns, ["path", "name", "idxcols"])
    assert idx.shape == (3, 3)
    np.testing.assert_array_equal(idx["idxcols"].agg(len), [2, 3, 1])


def test_read_pkg_index_errors(tmp_path):
    idxfile = tmp_path / "index.yaml"
    idxfile.touch()
    with pytest.raises(ValueError, match=f".*{idxfile.name}: bad index file"):
        read_pkg_index(idxfile)

    idxfile = idxfile.with_suffix(".txt")
    with pytest.raises(RuntimeError, match=f".*{idxfile.name}:.+"):
        read_pkg_index(idxfile)


@pytest.mark.parametrize(
    "csvfile, idxcols, ncatcols",
    [
        ("inputs/cost_energy_cap.csv", ["costs", "locs", "techs"], 3),
        ("inputs/energy_eff.csv", ["locs", "techs"], 2),
        ("inputs/names.csv", ["techs"], 1),
        ("outputs/capacity_factor.csv", ["carriers", "locs", "techs", "timesteps"], 3),
        ("outputs/resource_area.csv", ["locs", "techs"], 2),
    ],
)
def test_index_levels(csvfile, idxcols, ncatcols):
    pkgdir = Path("testing/files/mini-ex")
    _, coldict = index_levels(pkgdir / csvfile, idxcols)
    assert all(map(contains, idxcols, coldict))
    cols_w_vals = glom(
        coldict.values(),
        [match({"constraints": {"enum": lambda i: len(i) > 0}, str: str})],
    )
    # ncatcols: only categorical columns are inferred
    assert len(cols_w_vals) == ncatcols


@pytest.mark.parametrize("idx_t", [".yaml", ".json"])
def test_pkg_from_index(idx_t):
    meta = {
        "name": "foobarbaz",
        "title": "Foo Bar Baz",
        "keywords": ["foo", "bar", "baz"],
        "license": ["CC0-1.0"],
    }
    idxpath = Path("testing/files/mini-ex/index").with_suffix(idx_t)
    pkgdir, pkg, _ = pkg_from_index(meta, idxpath)
    assert pkgdir == idxpath.parent
    assert len(pkg["resources"]) == 5  # number of datasets
    indices = glom(pkg, ("resources", Iter().map("schema.primaryKey").all()))
    assert len(indices) == 5
    # FIXME: not sure what else to check


def test_pkg_from_index_skip_rows():
    meta = {
        "name": "foobarbaz",
        "title": "Foo Bar Baz",
        "keywords": ["foo", "bar", "baz"],
        "license": ["CC0-1.0"],
    }
    with pytest.warns(RuntimeWarning, match=".+: not in registry"):
        _, pkg, idx = pkg_from_index(meta, "testing/files/skip_test/index.yaml")
    assert "skip" in idx.columns
    expected = ["timesteps", "UK", "Ireland", "France"]
    assert glom(pkg, ("resources.0.schema.fields", Iter("name").all())) == expected


@pytest.mark.parametrize("idx_t", [".yaml", ".json"])
def test_pkg_glossary(idx_t):
    pkgdir = Path("testing/files/mini-ex")
    pkg = read_pkg(pkgdir / "datapackage.json")
    idx = read_pkg_index(pkgdir / f"index{idx_t}")
    glossary = pkg_glossary(pkg, idx)
    assert all(glossary.columns == ["path", "name", "idxcols", "values"])
    assert glossary["values"].apply(lambda i: isinstance(i, list)).all()
    assert len(glossary["path"].unique()) <= glossary.shape[0]


def test_pkg_from_files():
    meta = {
        "name": "foobarbaz",
        "title": "Foo Bar Baz",
        "keywords": ["foo", "bar", "baz"],
        "license": ["CC0-1.0"],
    }
    pkgdir = Path("testing/files/mini-ex")
    files = list(chain(pkgdir.glob("inputs/*"), pkgdir.glob("outputs/*")))
    # with index file
    _, pkg, idx = pkg_from_files(meta, pkgdir / "index.json", files)
    # files not in index: inheritance.csv, and loc_coordinates.csv
    assert len(pkg["resources"]) - len(idx) == 2

    # with directory containing index file
    with pytest.warns(RuntimeWarning, match="multiple indices:.+"):
        _pkgdir, pkg, idx = pkg_from_files(meta, pkgdir, files)
    assert _pkgdir == pkgdir
    assert len(pkg["resources"]) - len(idx) == 2


def test_pkg_from_files_no_index():
    meta = {
        "name": "foobarbaz",
        "title": "Foo Bar Baz",
        "keywords": ["foo", "bar", "baz"],
        "license": ["CC0-1.0"],
    }
    pkgdir = Path("testing/files/random")
    with pytest.warns(RuntimeWarning, match=".+no index file.+"):
        _, pkg, idx = pkg_from_files(meta, pkgdir, pkgdir.glob("data/*"))
    assert idx is None
    assert len(pkg["resources"]) == 3


def test_idxpath_from_pkgpath(tmp_path):
    idxpath = tmp_path / "index.json"
    with pytest.warns(RuntimeWarning, match=f".*{tmp_path.name}: no index file found"):
        assert idxpath_from_pkgpath(tmp_path) == ""

    idxpath.touch()
    assert idxpath_from_pkgpath(tmp_path) == idxpath

    idxpath.with_suffix(".yaml").touch()
    idxpath.with_suffix(".yml").touch()
    with pytest.warns(RuntimeWarning, match="multiple indices:.+"):
        # NOTE: returns the lexicographically first match
        assert idxpath_from_pkgpath(tmp_path) == idxpath.with_suffix(".json")


def test_write_pkg(pkg, tmp_path):
    res = write_pkg(pkg, tmp_path)
    assert len(res) == 1
    assert res[0].exists()

    assert not (tmp_path / "index.json").exists()
    assert not (tmp_path / "glossary.json").exists()


def test_write_pkg_idx_glossary(pkg, tmp_path):
    idx = read_pkg_index(f"{pkg.basepath}/index.json")
    glossary = pkg_glossary(pkg, idx)

    res = write_pkg(pkg, tmp_path, idx=idx, glossary=glossary)
    assert len(res) == 3
    assert all([p.exists() for p in res])

    with pytest.raises(TypeError):
        write_pkg(pkg, tmp_path, idx, glossary)


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
