from pathlib import Path
from glom import glom, Match

import pandas as pd
import pytest

from friendly_data.converters import to_df
from friendly_data.dpkg import res_from_entry
from friendly_data.io import dwim_file
from friendly_data.iamc import IAMconv


def test_iamconv():
    pkgdir = Path("testing/files/iamc")
    # - config: defaults for scenario & year, index names for carriers & technology
    # - index: 1 dummy.csv entry, 3 entries w/ aggregation, 1 regular
    confpath, idxpath = pkgdir / "config.yaml", pkgdir / "index.yaml"
    conv = IAMconv.from_file(confpath, idxpath)

    config = dwim_file(confpath)["indices"]  # type: ignore[call-overload]
    index = dwim_file(idxpath)
    assert config.keys() == conv.indices.keys()
    assert len(index) - 1 == len(conv.res_idx)
    assert conv.basepath == pkgdir

    # list of paths includes non-existent dummy file
    df = conv.to_df([e["path"] for e in index])

    # FIXME: check aggregations

    # check file with no aggregation
    entry, *_ = [e for e in conv.res_idx if not e.get("agg")]
    ref = to_df(res_from_entry(entry, pkgdir))
    fmt = entry["iamc"]
    techs = conv.read_indices(config["technology"], pkgdir)
    for name, title in techs.iteritems():  # result: dpkg -> iamc
        var = fmt.format(technology=title)
        result = df.query(f"variable == {var!r}")
        expected = ref.query(f"technology == '{name}'")
        assert len(expected) == len(result)


@pytest.mark.parametrize(
    "wide",
    [False, pytest.param(True, marks=pytest.mark.xfail(reason="FIXME: bad test data"))],
)
def test_iamconv_to_csv(wide, iamconv, tmp_path):
    iamc_csv = tmp_path / "iamc.csv"

    iamconv.to_csv([fp for fp in iamconv.res_idx.get("path")], iamc_csv, wide=wide)
    assert iamc_csv.exists()  # FIXME: better test
    iamc_csv.unlink()


def test_iamconv_to_df_from_files(iamconv):
    df = iamconv.to_df([fp for fp in iamconv.res_idx.get("path")])
    assert not df.isna().any().any()


def test_iamconv_to_df_from_dfs(iamconv):
    dfs = {
        entry["name"]: to_df(res_from_entry(entry, iamconv.basepath))
        for entry in iamconv.res_idx
    }
    df = iamconv.to_df(dfs)
    assert not df.isna().any().any()


def test_iamconv_frames(iamconv):
    entry = iamconv.res_idx[0]
    res = iamconv.frames(entry, to_df(res_from_entry(entry, iamconv.basepath)))
    assert glom(res, Match([pd.DataFrame]))


def test_iamconv_match(iamconv):
    assert iamconv._match_item("foo.csv") == tuple()  # no match

    res = iamconv._match_item("nameplate_capacity.csv")  # exist in example data
    assert glom(res, Match((dict, pd.DataFrame)))

    entry, *_ = [i for i in iamconv.res_idx if i["name"] == "nameplate_capacity"]
    df = to_df(res_from_entry(entry, iamconv.basepath))
    res = iamconv._match_item(("nameplate_capacity", df))
    assert glom(res, Match((dict, pd.DataFrame)))


def test_iamconv_iamcify(iamconv):
    df = (
        to_df(res_from_entry(iamconv.res_idx[0], iamconv.basepath))
        .assign(variable="Foo|Bar|Baz")
        .pipe(iamconv.resolve_idxcol_defaults)
    )
    assert all([i in df.index.names or i in df.columns for i in iamconv._IAMC_IDX])


def test_iamconv_agg_vals_all(iamconv):
    _entry = {
        "path": "foo.csv",
        "idxcols": ["foo", "bar", "baz"],
        "agg": {
            "foo": [
                {"values": ["xxx", "yyy", "zzz"], "variable": "bla bla"},
                {"values": ["ppp"], "variable": "foo bar"},
            ],
            "bar": [{"values": ["aaa", "bbb", "ccc"], "variable": "bla bla"}],
        },
    }
    with pytest.raises(AssertionError, match="one column"):
        iamconv.agg_vals_all(_entry)

    _entry["agg"].popitem()  # pop "bar"
    col, vals = iamconv.agg_vals_all(_entry)
    assert col == "foo"
    assert len(vals) == 4


def test_iamconv_index_levels(iamconv):
    assert iamconv.index_levels(["technology"])
    with pytest.raises(ValueError, match="idxcols.+only for user defined"):
        iamconv.index_levels(["model", "scenario"])
