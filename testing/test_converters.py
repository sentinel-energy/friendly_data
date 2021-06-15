import pyam
from friendly_data.io import dwim_file

from glom import Assign, glom, Iter, T
import numpy as np
import pandas as pd
import pytest

from friendly_data.converters import IAMconv
from friendly_data.converters import from_df
from friendly_data.converters import from_dst
from friendly_data.converters import to_df
from friendly_data.dpkg import pkg_from_index

from .conftest import assert_log, expected_schema


@pytest.mark.skip(reason="not sure how to test schema parsing")
def test_schema_parsing():
    pass


def test_pkg_to_df(rnd_pkg):
    for resource in rnd_pkg.resources:
        df = to_df(resource)  # test target, don't touch this
        from_impl = expected_schema(df, type_map={})
        # read from file; strings are read as `object`, remap to `string`
        raw = expected_schema(resource, type_map={"object": "string", "int64": "Int64"})
        # impl marks columns as timestamps based on the schema.  so remap
        # timestamp columns as datetime64[ns] as per the schema
        ts_cols = [
            field.name for field in resource.schema.fields if "datetime" in field.type
        ]
        raw.update((col, "datetime64[ns]") for col in ts_cols)
        assert from_impl == raw

        if not ts_cols:  # no timestamps, skip
            continue

        # resource w/ timestamps
        dtype_cmp = df[ts_cols].dtypes == np.dtype("datetime64[ns]")
        assert dtype_cmp.all(axis=None)

    # resource w/ a index
    resource = rnd_pkg.resources[0]
    field_names = [field.name for field in resource.schema.fields]
    glom(resource, Assign("schema.primaryKey", field_names[0]))
    df = to_df(resource)
    # compare columns
    assert list(df.columns) == field_names[1:]
    # check if the right column has been set as index
    assert df.index.name == resource.schema.fields[0].name

    # resource w/ a MultiIndex
    glom(resource, Assign("schema.primaryKey", field_names[:2]))
    df = to_df(resource)
    # compare columns
    assert list(df.columns) == field_names[2:]
    # check if the right column has been set as index
    assert df.index.names == field_names[:2]

    # resource w/ NA
    resource = rnd_pkg.resources[1]
    # set new NA value: "sit" from "Lorem ipsum dolor sit amet consectetur
    # adipiscing", TRE - 2nd column
    glom(resource, Assign("schema.missingValues", ["", "sit"]))
    df = to_df(resource)
    assert df.isna().any(axis=None)

    # unsupported resource type
    resource = rnd_pkg.resources[0]
    update = {
        "path": resource["path"].replace("csv", "txt"),
        "mediatype": resource["mediatype"].replace("csv", "plain"),
    }
    resource.update(update)
    with pytest.raises(ValueError, match="unsupported source.+"):  # default behaviour
        df = to_df(resource)
    assert to_df(resource, noexcept=True).empty  # suppress exceptions


def test_pkg_to_df_skip_rows(pkg_meta):
    _, pkg, __ = pkg_from_index(pkg_meta, "testing/files/skip_test/index.yaml")
    df = to_df(pkg["resources"][0])
    expected = ["UK", "Ireland", "France"]
    np.testing.assert_array_equal(df.columns, expected)
    assert isinstance(df.index, pd.DatetimeIndex)


def test_pkg_to_df_aliased_cols(pkg_w_alias):
    df = to_df(pkg_w_alias["resources"][1])
    assert "region" in df.index.names
    assert "flow_in" in df.columns


def test_df_to_resource(tmp_path, pkg_w_alias):
    df = to_df(pkg_w_alias["resources"][1])
    res = from_df(df, basepath=tmp_path)
    fpath = f"{'_'.join(df.columns)}.csv"
    assert (tmp_path / fpath).exists()
    assert fpath == res["path"]

    df.columns = ["unit", "energy_in"]
    df.index.names = ["technology", "node"]
    alias = {"node": "region", "energy_in": "flow_in"}
    res = from_df(df, basepath=tmp_path, alias=alias)
    res_alias = glom(
        res,
        (
            "schema.fields",
            Iter()
            .filter(lambda i: "alias" in i)
            .map(({1: "name", 2: "alias"}, T.values()))
            .all(),
            dict,
        ),
    )
    assert res_alias == alias


@pytest.mark.skip
def test_dst_to_pkg(tmp_path, pkg_w_alias):
    pass


def test_iamconv(tmp_iamc):
    _, pkgdir = tmp_iamc
    confpath, idxpath = pkgdir / "config.yaml", pkgdir / "index.yaml"
    conv = IAMconv.from_file(confpath, idxpath)

    config = dwim_file(confpath)["indices"]
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
