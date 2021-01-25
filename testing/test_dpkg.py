from contextlib import nullcontext as does_not_raise
from itertools import chain
import json
from operator import contains
from pathlib import Path

from glom import glom, Iter, T
import numpy as np
import pytest

from sark.converters import _schema, _source_type
from sark.dpkg import create_pkg
from sark.dpkg import index_levels
from sark.dpkg import idxpath_from_pkgpath
from sark.dpkg import pkg_from_files
from sark.dpkg import pkg_from_index
from sark.dpkg import pkg_glossary
from sark.dpkg import read_pkg
from sark.dpkg import read_pkg_index
from sark.dpkg import registry
from sark.dpkg import update_pkg
from sark.dpkg import write_pkg
from sark.helpers import match, select
from sark.metatools import get_license

from .conftest import expected_schema


class noop_map(dict):
    def __getitem__(self, key):
        return key


def test_source_type_heuristics():
    with pytest.raises(ValueError):
        _source_type("/path/to/non-existent-file.ext")


def test_pkg_creation():
    pkgdir = Path("testing/files/random")
    pkg_meta = {"name": "test", "licenses": get_license("CC0-1.0")}
    csvs = [f.relative_to(pkgdir) for f in (pkgdir / "data").glob("sample-ok-?.csv")]
    pkg = create_pkg(pkg_meta, csvs, pkgdir)
    for resource in pkg.resources:
        assert _schema(resource, noop_map()) == expected_schema(resource)


def test_pkg_read():
    pkgdir = Path("testing/files/random")
    dpkg_json = pkgdir / "datapackage.json"
    pkg = read_pkg(dpkg_json)
    assert all(Path(res.source).exists() for res in pkg.resources)


def test_zippkg_read(rnd_pkg, tmp_path_factory):
    with tmp_path_factory.mktemp("ziptest-") as tmpdir:
        zipfile = tmpdir / "testpackage.zip"
        rnd_pkg.save(f"{zipfile}")

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
        rnd_pkg.descriptor,
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
        rnd_pkg.descriptor,
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
        rnd_pkg.descriptor,
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
    np.testing.assert_array_equal(idx.columns, ["file", "name", "idxcols"])
    assert idx.shape == (3, 3)
    np.testing.assert_array_equal(idx["idxcols"].agg(len), [2, 3, 1])


def test_read_pkg_index_errors(tmp_path):
    idxfile = tmp_path / "index.yaml"
    idxfile.touch()
    with pytest.raises(ValueError, match=f"{idxfile}: bad index file"):
        read_pkg_index(idxfile)

    idxfile = idxfile.with_suffix(".txt")
    with pytest.raises(RuntimeError, match=f"{idxfile}:.+"):
        read_pkg_index(idxfile)


@pytest.mark.parametrize(
    "col, col_t, expectation",
    [
        ("locs", "idxcols", does_not_raise()),
        ("storage", "cols", does_not_raise()),
        (
            "notinreg",
            "cols",
            pytest.warns(RuntimeWarning, match=f"notinreg: not in registry"),
        ),
        (
            "timesteps",
            "bad_col_t",
            pytest.raises(ValueError, match=f"bad_col_t: unknown column type"),
        ),
    ],
)
def test_registry(col, col_t, expectation):
    with expectation:
        res = registry(col, col_t)
        assert isinstance(res, dict)
        if col == "notinreg":
            assert res == {}


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
    assert len(pkg.descriptor["resources"]) == 5  # number of datasets
    indices = glom(pkg.descriptor, ("resources", Iter().map("schema.primaryKey").all()))
    assert len(indices) == 5
    # FIXME: not sure what else to check


@pytest.mark.parametrize("idx_t", [".yaml", ".json"])
def test_pkg_glossary(idx_t):
    pkgdir = Path("testing/files/mini-ex")
    pkg = read_pkg(pkgdir / "datapackage.json")
    idx = read_pkg_index(pkgdir / f"index{idx_t}")
    glossary = pkg_glossary(pkg, idx)
    assert all(glossary.columns == ["file", "name", "idxcols", "values"])
    assert glossary["values"].apply(lambda i: isinstance(i, list)).all()
    assert len(glossary["file"].unique()) <= glossary.shape[0]


def test_pkg_from_files():
    meta = {
        "name": "foobarbaz",
        "title": "Foo Bar Baz",
        "keywords": ["foo", "bar", "baz"],
        "license": ["CC0-1.0"],
    }
    pkgdir = Path("testing/files/mini-ex")
    files = chain(pkgdir.glob("inputs/*"), pkgdir.glob("outputs/*"))
    _, pkg, idx = pkg_from_files(meta, pkgdir / "index.json", files)
    # files not in index: inheritance.csv, and loc_coordinates.csv
    assert len(pkg.descriptor["resources"]) - len(idx) == 2


def test_idxpath_from_pkgpath(tmp_path):
    idxpath = tmp_path / "index.yml"
    with pytest.warns(RuntimeWarning, match=f"{tmp_path}: no index file found"):
        assert idxpath_from_pkgpath(tmp_path) == ""

    idxpath.touch()
    assert idxpath_from_pkgpath(tmp_path) == idxpath

    idxpath.with_suffix(".yaml").touch()
    idxpath.with_suffix(".json").touch()
    with pytest.warns(RuntimeWarning, match=f"multiple indices:.+"):
        # NOTE: the newest file is returned
        assert idxpath_from_pkgpath(tmp_path) == idxpath.with_suffix(".json")


def test_write_pkg(pkg, tmp_path):
    res = write_pkg(pkg, tmp_path)
    assert len(res) == 1
    assert res[0].exists()

    assert not (tmp_path / "index.json").exists()
    assert not (tmp_path / "glossary.json").exists()


def test_write_pkg_idx_glossary(pkg, tmp_path):
    idx = read_pkg_index(f"{pkg.base_path}/index.json")
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
