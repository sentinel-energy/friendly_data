from itertools import product
import json
from pathlib import Path
from typing import cast, List

from glom import glom, Iter
import pytest

from friendly_data.cli import _metadata, generate_index_file
from friendly_data.cli import create
from friendly_data.cli import describe
from friendly_data.cli import list_licenses
from friendly_data.cli import license_info
from friendly_data.cli import _create
from friendly_data.cli import _rm_from_idx
from friendly_data.cli import _rm_from_pkg
from friendly_data.cli import _rm_from_disk
from friendly_data.cli import remove
from friendly_data.cli import _update
from friendly_data.cli import update
from friendly_data.cli import to_iamc
from friendly_data.io import dwim_file

from .conftest import assert_log, chdir


def test_metadata(caplog):
    mandatory = ["name", "licenses"]
    res = _metadata(
        mandatory,
        name="",
        title="foo bar",
        licenses="CC0-1.0",
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
        licenses="",
        description="",
        keywords="",
    )
    assert dict() == res

    mandatory = ["name", "keywords"]
    # FIXME: don't know why 'match' isn't working
    # with pytest.raises(ValueError, match=f"{mandatory}:.+"):
    with pytest.raises(SystemExit) as err:
        _metadata(
            mandatory,
            name="",
            title="",
            licenses="",
            description="",
            keywords="",
        )
    assert err.value.code == 1
    assert_log(caplog, f"{mandatory}: mandatory metadata missing", "ERROR")


@pytest.mark.parametrize("ext", [".yaml", ".yml", ".json"])
def test_metadata_file(ext):
    conf = Path("testing/files/metadata/pkgmeta")
    mandatory = ["name", "licenses"]
    res = _metadata(
        mandatory,
        name="",
        title="",
        licenses="",
        description="",
        keywords="",
        metadata=conf.with_suffix(ext),
    )
    assert all(k in res for k in mandatory)


@pytest.mark.parametrize(
    "src, export", list(product(("index.yaml", ""), ("", "outdir")))
)
def test_create(tmp_pkgdir_w_files, caplog, src, export):
    dest, dpkgjson, meta, files = tmp_pkgdir_w_files

    if export:
        export = dest.parent / export
        dpkgjson = export / "datapackage.json"
    else:
        dpkgjson.unlink()

    assert _create(meta, dest / src, files, export=export)
    assert dpkgjson.exists()
    if not src:
        assert_log(caplog, "multiple indices", "WARNING")
    assert glom(dwim_file(dpkgjson), ("resources", len)) == 7


@pytest.mark.parametrize("export", ("", "outdir"))
def test_create_no_idx(tmp_pkgdir_w_files, export):
    dest, dpkgjson, meta, files = tmp_pkgdir_w_files

    if export:
        export = dest.parent / export
        dpkgjson = export / "datapackage.json"
    else:
        dpkgjson.unlink()

    for i in dest.glob("index.*"):
        i.unlink()  # delete index files

    assert _create(meta, dest, files, export=export)
    assert dpkgjson.exists()
    assert glom(dwim_file(dpkgjson), ("resources", len)) == 3


def test_create_error(tmp_pkgdir_w_files, caplog):
    dest, _, meta, files = tmp_pkgdir_w_files

    with pytest.raises(SystemExit) as err:
        create(dest, *files, inplace=False, export="", **meta)
    assert err.value.code == 1
    assert_log(caplog, "choose between `inplace` or `export`", "ERROR")


def test_create_warning(tmp_pkgdir_w_files, caplog):
    dest, _, meta, files = tmp_pkgdir_w_files

    assert create(dest, *files, inplace=True, export="out", **meta)
    assert_log(caplog, "`inplace` will be ignored", "WARNING")


def test_update(tmp_pkgdir):
    _, dest = tmp_pkgdir
    meta = {"name": "Howzah", "licenses": "CC0-1.0"}
    assert update(dest, **meta)
    assert meta == glom(
        dwim_file(dest / "datapackage.json"),
        {"name": "name", "licenses": "licenses.0.name"},
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
    assert update(dest, dest / meta["path"])
    entry, *_ = glom(
        dwim_file(dest / "datapackage.json"),
        (
            "resources",
            Iter().filter(lambda i: meta["path"] == i["path"]).all(),
        ),
    )
    assert glom(entry, "schema.primaryKey") == meta["idxcols"]


def test_update_add(tmp_pkgdir):
    _, dest = tmp_pkgdir
    dpkgjson = dest / "datapackage.json"
    pkg = dwim_file(dpkgjson)
    count = len(pkg["resources"])
    files = [
        dest / f"inputs/{f}"
        for f in ("description.csv", "inheritance.csv", "loc_coordinates.csv")
    ]
    assert _update(pkg, dest, files)
    pkg = dwim_file(dpkgjson)  # description.csv is already included
    assert len(pkg["resources"]) == count + 2


@pytest.mark.skip(reason="probably, not relevant")
def test_update_badfile(tmp_pkgdir, caplog):
    _, dest = tmp_pkgdir
    pkgjson = json.loads((dest / "datapackage.json").read_text())
    count = glom(pkgjson, ("resources", len))
    files = [dest / f"inputs/{f}" for f in ("inheritance.csv", "nonexistent.csv")]
    assert _update(pkgjson, dest, files)
    assert_log(caplog, f"{files[-1].name}: skipped", "WARNING")
    assert glom(pkgjson, ("resources", len)) == count + 1


def test_rm_from_et_al(tmp_pkgdir_w_files):
    dest, dpkgjson, *_ = tmp_pkgdir_w_files
    dstpath = dest / "inputs/description.csv"

    idx = _rm_from_idx(dest, [dstpath])
    assert "inputs/description.csv" not in glom(idx, ["path"])

    pkg1 = dwim_file(dpkgjson)
    pkg2 = _rm_from_pkg(dwim_file(dpkgjson), dest, [dstpath])
    assert len(pkg1["resources"]) - len(pkg2["resources"]) == 1


def test_rm_from_disk(tmp_path):
    files = []
    for f in ("foo", "bar", "baz"):
        files.append(tmp_path / f)
        files[-1].touch()

    _rm_from_disk(files)
    assert all(map(lambda f: not f.exists(), files))


def test_remove(tmp_pkgdir):
    _, dest = tmp_pkgdir
    msg = remove(dest, dest / "inputs/energy_eff.csv")
    assert msg and msg.count("json") == 1
    assert msg and msg.count("yaml") == 1


def test_license_display(caplog):
    tokens = ("domain", "id", "maintainer", "Apache", "GPL", "CC")
    table = list_licenses()
    assert all(map(lambda t: t in table, tokens))

    assert isinstance(license_info("CC0-1.0"), dict)

    bad_license = "not-there"
    with pytest.raises(SystemExit) as err:
        license_info(bad_license)
    assert err.value.code == 1
    assert_log(caplog, f"no matching license with id: {bad_license}", "ERROR")


def test_generate_index_file(tmp_iamc):
    _, pkgdir = tmp_iamc
    idxpath = pkgdir / "index.yaml"
    idxpath.unlink()
    # matches: {,annual_cost_per_}nameplate_capacity.csv
    fpaths = [f.relative_to(pkgdir) for f in pkgdir.glob("*nameplate*.csv")]
    with chdir(pkgdir):
        generate_index_file(*fpaths)
    assert idxpath.exists()
    idx = dwim_file(idxpath)
    assert len(idx) == 2
    # nameplate_capacity: scenario, unit, year
    # annual_cost_per_nameplate_capacity: unit
    assert glom(idx, Iter("idxcols").map(len).all()) == [3, 1]


def test_iamc(tmp_iamc):
    _, pkgdir = tmp_iamc
    confpath = pkgdir / "config.yaml"
    idxpath = pkgdir / "index.yaml"
    newiamc = pkgdir / "iamc-out.csv"
    assert to_iamc(confpath, idxpath, newiamc)
    assert newiamc.exists()


def test_describe(tmp_pkgdir, caplog):
    _, pkgdir = tmp_pkgdir
    txt = describe(pkgdir)
    assert txt
    tokens = ("name", "licenses", "resources", "path", "fields", "csv")
    assert all(map(lambda t: t in t, tokens))

    pkg_json = pkgdir / "datapackage.json"
    pkg_json.unlink()
    with pytest.raises(SystemExit):
        describe(pkgdir)
    assert_log(caplog, f"{pkg_json}: not found", "ERROR")
