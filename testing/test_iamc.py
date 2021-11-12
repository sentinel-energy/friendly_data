import pytest

from friendly_data.converters import to_df
from friendly_data.dpkg import res_from_entry
from friendly_data.io import dwim_file
from friendly_data.iamc import IAMconv


def test_iamconv(tmp_iamc):
    _, pkgdir = tmp_iamc
    # - config: defaults for scenario & year, index names for carriers & technology
    # - index: 1 dummy.csv entry, 3 entries w/ aggregation, 1 regular
    confpath, idxpath = pkgdir / "config.yaml", pkgdir / "index.yaml"
    conv = IAMconv.from_file(confpath, idxpath)

    config = dwim_file(confpath)["indices"]  # type: ignore[call-overload]
    index = dwim_file(idxpath)
    assert config.keys() == conv.indices.keys()
    assert len(index) - 1 == len(conv.res_idx)
    assert conv.basepath == pkgdir

    df = conv.to_df([e["path"] for e in index])
    assert not df.isna().any().any()

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
        conv.agg_vals_all(_entry)

    _entry["agg"].popitem()
    col, vals = conv.agg_vals_all(_entry)
    assert col == "foo"
    assert len(vals) == 4
