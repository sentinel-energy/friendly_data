import json
from pathlib import Path
from typing import cast, List

from glom import glom, Iter
import pytest

from friendly_data.cli import _metadata, remove
from friendly_data.cli import _create
from friendly_data.cli import _rm_from_idx
from friendly_data.cli import _rm_from_glossary
from friendly_data.cli import _rm_from_pkg
from friendly_data.cli import add
from friendly_data.cli import update
from friendly_data.dpkg import read_pkg
from friendly_data.io import dwim_file


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
        for f in ("description.csv", "inheritance.csv", "loc_coordinates.csv")
    ]
    assert _create({"name": "foo", "license": "CC0-1.0"}, dest / "index.yaml", *files)
    assert (dest / "datapackage.json").exists() and (dest / "glossary.json").exists()

    # with package directory only
    (dest / "datapackage.json").unlink()
    (dest / "glossary.json").unlink()
    with pytest.warns(RuntimeWarning, match="multiple indices.+"):
        assert _create({"name": "foo", "license": "CC0-1.0"}, dest, *files)
    assert (dest / "datapackage.json").exists() and (dest / "glossary.json").exists()


def test_add(tmp_pkgdir):
    _, dest = tmp_pkgdir
    pkgjson = dest / "datapackage.json"
    count = glom(json.loads(pkgjson.read_text()), ("resources", len))
    files = [
        dest / f"inputs/{f}"
        for f in ("description.csv", "inheritance.csv", "loc_coordinates.csv")
    ]
    assert add(dest, *files)
    # description.csv is already included
    assert glom(json.loads(pkgjson.read_text()), ("resources", len)) == count + 2


def test_add_badfile(tmp_pkgdir):
    _, dest = tmp_pkgdir
    pkgjson = dest / "datapackage.json"
    count = glom(json.loads(pkgjson.read_text()), ("resources", len))
    files = [dest / f"inputs/{f}" for f in ("inheritance.csv", "nonexistent.csv")]
    with pytest.warns(RuntimeWarning, match=f"{files[-1].name}: skipped.+"):
        assert add(dest, *files)
    assert glom(json.loads(pkgjson.read_text()), ("resources", len)) == count + 1


def test_update(tmp_pkgdir):
    _, dest = tmp_pkgdir
    meta = {"name": "Howzah", "license": "Public Domain"}
    assert update(dest, **meta)
    assert meta == glom(
        dwim_file(dest / "datapackage.json"),
        {"name": "name", "license": "license"},
    )

    meta = {
        "path": "inputs/inheritance.csv",
        "name": "inheritance",
        "idxcols": ["technology"],
    }
    idx = cast(List, dwim_file(dest / "index.yaml")) + [meta]
    # there are multiple index files in the test pkg, write to both as which
    # file is read isn't deterministic
    [dwim_file(dest / f, idx) for f in ("index.yaml", "index.json")]
    with pytest.warns(RuntimeWarning):  # multiple index files in test pkg
        assert update(dest, dest / meta["path"])
    entry, *_ = glom(
        dwim_file(dest / "datapackage.json"),
        (
            "resources",
            Iter().filter(lambda i: meta["path"] == i["path"]).all(),
        ),
    )
    assert glom(entry, "schema.primaryKey") == meta["idxcols"]


def test_rm_from_et_al(tmp_pkgdir):
    _, dest = tmp_pkgdir
    dstpath = dest / "inputs/description.csv"

    with pytest.warns(RuntimeWarning):  # multiple indices
        idx = _rm_from_idx(dest, dstpath)
    assert "inputs/description.csv" not in glom(idx, ["path"])

    glossary = _rm_from_glossary(dest, dstpath)
    assert "inputs/description.csv" not in glossary["path"].unique()

    (dest / "glossary.json").unlink()  # pkg w/o glossary
    assert _rm_from_glossary(dest, dstpath) is None

    pkg1 = read_pkg(dest)
    pkg2 = _rm_from_pkg(dest, dstpath)
    assert len(pkg1.resources) - len(pkg2.resources) == 1


def test_remove(tmp_pkgdir):
    _, dest = tmp_pkgdir
    with pytest.warns(RuntimeWarning):  # multiple indices
        msg = remove(dest, dest / "inputs/description.csv")  # pkg w/ glossary
    assert msg and msg.count("json") == 2
    assert msg and msg.count("yaml") == 1

    (dest / "glossary.json").unlink()  # pkg w/o glossary
    with pytest.warns(RuntimeWarning):  # multiple indices
        msg = remove(dest, dest / "inputs/energy_eff.csv")
    assert msg and msg.count("json") == 1
    assert msg and msg.count("yaml") == 1
