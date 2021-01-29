from pathlib import Path
from typing import cast, Dict, List

from glom import glom, Iter
import pytest

from sark._types import _path_t
from sark.cli import _metadata, remove
from sark.cli import _create
from sark.cli import _rm_from_idx
from sark.cli import _rm_from_glossary
from sark.cli import _rm_from_pkg
from sark.cli import add
from sark.cli import update
from sark.dpkg import read_pkg
from sark.io import dwim_file


def test_metadata():
    mandatory = ["name", "license"]
    res = _metadata(
        mandatory,
        name="",
        title="foo bar",
        license="CC0-1.0",
        description="",
        keywords="foo bar",
    )
    assert all(k in res for k in mandatory)
    assert res["name"] == "foo_bar"
    assert res["keywords"] == ["foo", "bar"]
    assert all(res.values())

    # no mandatory keys
    res = _metadata(
        [],
        name="",
        title="",
        license="",
        description="",
        keywords="",
    )
    assert dict() == res

    mandatory = ["name", "keywords"]
    # FIXME: don't know why 'match' isn't working
    # with pytest.raises(ValueError, match=f"{mandatory}:.+"):
    with pytest.raises(ValueError):
        _metadata(
            mandatory,
            name="",
            title="",
            license="",
            description="",
            keywords="",
        )


@pytest.mark.parametrize("ext", [".yaml", ".yml", ".json"])
def test_metadata_file(ext):
    conf = Path("testing/files/metadata/pkgmeta")
    mandatory = ["name", "license"]
    res = _metadata(
        mandatory,
        name="",
        title="",
        license="",
        description="",
        keywords="",
        metadata=conf.with_suffix(ext),
    )
    assert all(k in res for k in mandatory)


def test_create(tmp_pkgdir):
    _, dest = tmp_pkgdir
    (dest / "datapackage.json").unlink()
    (dest / "glossary.json").unlink()
    files = [
        dest / f"inputs/{f}"
        for f in ("names.csv", "inheritance.csv", "loc_coordinates.csv")
    ]
    assert _create({"name": "foo", "license": "CC0-1.0"}, dest / "index.yaml", *files)
    assert (dest / "datapackage.json").exists() and (dest / "glossary.json").exists()


def nresources(pkgjson: _path_t):
    return len(cast(Dict, dwim_file(pkgjson))["resources"])


def test_add(tmp_pkgdir):
    _, dest = tmp_pkgdir
    pkgjson = dest / "datapackage.json"
    count = nresources(pkgjson)
    files = [
        dest / f"inputs/{f}"
        for f in ("names.csv", "inheritance.csv", "loc_coordinates.csv")
    ]
    assert add(dest, *files)
    # names.csv is already included
    assert nresources(pkgjson) == count + 2


def test_add_badfile(tmp_pkgdir):
    _, dest = tmp_pkgdir
    pkgjson = dest / "datapackage.json"
    count = nresources(pkgjson)
    files = [dest / f"inputs/{f}" for f in ("inheritance.csv", "nonexistent.csv")]
    with pytest.warns(RuntimeWarning, match=f"{files[-1].name}: skipped.+"):
        assert add(dest, *files)
    assert nresources(pkgjson) == count + 1


def test_update(tmp_pkgdir):
    _, dest = tmp_pkgdir
    meta = {"name": "Howzah", "license": "Public Domain"}
    assert update(dest, **meta)
    assert meta == glom(
        dwim_file(dest / "datapackage.json"),
        {"name": "name", "license": "license"},
    )

    meta = {
        "file": "inputs/inheritance.csv",
        "name": "inheritance",
        "idxcols": ["techs"],
    }
    idx = cast(List, dwim_file(dest / "index.yaml")) + [meta]
    # there are multiple index files in the test pkg, write to both as which
    # file is read isn't deterministic
    [dwim_file(dest / f, idx) for f in ("index.yaml", "index.json")]
    with pytest.warns(RuntimeWarning):  # multiple index files in test pkg
        assert update(dest, dest / meta["file"])
    dpkgjson = dwim_file(dest / "datapackage.json")
    entry, *_ = glom(
        dpkgjson,
        (
            "resources",
            Iter().filter(lambda i: meta["file"] == i["path"]).all(),
        ),
    )
    assert glom(entry, "schema.primaryKey") == meta["idxcols"]


def test_rm_from_et_al(tmp_pkgdir):
    _, dest = tmp_pkgdir
    dstpath = dest / "inputs/names.csv"

    with pytest.warns(RuntimeWarning):  # multiple indices
        idx = _rm_from_idx(dest, dstpath)
    assert "inputs/names.csv" not in idx["file"].unique()

    glossary = _rm_from_glossary(dest, dstpath)
    assert "inputs/names.csv" not in glossary["file"].unique()

    print("index in test:")
    print(idx["file"].unique())
    print("glossary in test:")
    print(glossary["file"].unique())

    (dest / "glossary.json").unlink()  # pkg w/o glossary
    assert _rm_from_glossary(dest, dstpath) is None

    pkg1 = read_pkg(dest)
    pkg2 = _rm_from_pkg(dest, dstpath)
    assert len(pkg1.resources) - len(pkg2.resources) == 1


def test_remove(tmp_pkgdir):
    _, dest = tmp_pkgdir

    print("dir listing:")
    print(list(dest.iterdir()))

    with pytest.warns(RuntimeWarning):  # multiple indices
        msg = remove(dest, dest / "inputs/names.csv")  # pkg w/ glossary
    assert msg and msg.count("json") == 3

    (dest / "glossary.json").unlink()  # pkg w/o glossary
    with pytest.warns(RuntimeWarning):  # multiple indices
        msg = remove(dest, dest / "inputs/energy_eff.csv")
    assert msg and msg.count("json") == 2


import sys


def posixpathstr(fpath: Path) -> str:
    return str(fpath.as_posix()) if sys.platform in ("win32", "cygwin") else str(fpath)


def test_windows(tmp_path):
    print(type(tmp_path), tmp_path, posixpathstr(tmp_path))

    mydir = tmp_path / "foo"
    mydir.mkdir()
    print(type(mydir), mydir, posixpathstr(mydir))

    myfile = tmp_path / "foo/bar.txt"
    myfile.touch()
    print(type(myfile), myfile, posixpathstr(myfile))

    assert False
