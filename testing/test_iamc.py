from glom import glom, Iter
import pyam

from friendly_data.converters import to_df
from friendly_data.io import dwim_file
from friendly_data.iamc import IAMconv


def test_iamconv(tmp_iamc):
    _, pkgdir = tmp_iamc
    confpath, idxpath = pkgdir / "config.yaml", pkgdir / "index.yaml"
    conv = IAMconv.from_file(confpath, idxpath)

    config = dwim_file(confpath)["indices"]  # type: ignore[call-overload]
    index = dwim_file(idxpath)
    assert config.keys() == conv.indices.keys()
    assert len(index) - 1 == len(conv.res_idx)  # one dummy entry
    assert conv.basepath == pkgdir

    iamdf = pyam.IamDataFrame(pkgdir / "mini.csv")
    exportdir = pkgdir / "outdir"
    resources = conv.from_iamdf(iamdf, exportdir)
    assert len(resources) == len(index) - 1
    assert all(
        glom(resources, Iter("path").map(lambda i: (exportdir / i).exists()).all())
    )

    # example test with nameplate_capacity
    cap_entry = conv.res_idx[0]
    cap_res = resources[0]
    assert cap_res["path"] == cap_entry["path"]
    techs = conv.read_indices(config["technology"], pkgdir)
    capacity = to_df(resources[0])
    df = iamdf.as_pandas()
    df2 = conv.to_df([cap_res["path"]], exportdir)

    # result1: iamc -> dpkg, result2: dpkg -> iamc
    for name, title in techs.iteritems():
        expected = df.query(f"variable == 'Capacity|Electricity|{title}'")
        result1 = capacity.query(f"technology == '{name}'")
        result2 = df2.query(f"variable == 'Capacity|Electricity|{title}'")
        assert len(expected) == len(result1) == len(result2)
