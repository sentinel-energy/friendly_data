from pathlib import Path

from glom import Assign, glom, Iter, T
import numpy as np
import pandas as pd
import pytest

from friendly_data.converters import from_df
from friendly_data.converters import from_dst
from friendly_data.converters import resolve_aliases
from friendly_data.converters import to_da
from friendly_data.converters import to_df
from friendly_data.converters import to_dst
from friendly_data.converters import to_mfdst
from friendly_data.converters import xr_metadata
from friendly_data.converters import xr_da
from friendly_data.dpkg import pkg_from_index, read_pkg, res_from_entry

from friendly_data.io import dwim_file

from .conftest import expected_schema, to_df_noalias


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


def test_resolve_aliases(pkg_w_alias):
    for res in pkg_w_alias.resources:
        _df, entry = to_df_noalias(res)
        df = resolve_aliases(_df, entry["alias"])
        assert "region" in df.index.names
        if "flow_in" in entry["path"]:
            assert "flow_in" in df.columns


def test_df_to_resource(tmp_path, pkg_w_alias):
    df = to_df(pkg_w_alias["resources"][1])
    res = from_df(df, basepath=tmp_path)
    fpath = f"{'_'.join(df.columns)}.csv"
    assert (tmp_path / fpath).exists()
    assert fpath == res["path"]

    df.columns = ["energy_in"]
    df.index.names = ["technology", "node", "unit"]
    alias = {"node": "region", "energy_in": "flow_in"}
    for r in (True, False):
        res = from_df(df, basepath=tmp_path, alias=alias, rename=r)
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

        if r:
            assert not res_alias
        else:
            assert res_alias == alias


def test_xr_metadata(pkg_w_alias):
    # 1: alias, unit, 2: alias
    df1, df2 = [to_df(res) for res in pkg_w_alias.resources]

    df1_res, coords1, attrs1 = xr_metadata(df1)
    assert df1.index.names == df1_res.index.names
    assert set(df1.index.names) == set(coords1)
    assert attrs1 == {}

    df2_res, coords2, attrs2 = xr_metadata(df2)
    assert set(df2.index.names) - set(df2_res.index.names) == {"unit"}
    assert set(df2.index.names) - set(coords2) == {"unit"}
    assert set(attrs2) == {"unit"}


def test_xr_da(pkg_w_alias):
    # 1: alias, 2: alias, unit
    df1, df2 = [to_df(res) for res in pkg_w_alias.resources]

    df_aligned, coords, attrs = xr_metadata(df1)
    arr1 = xr_da(df_aligned, 0, coords=coords, attrs=attrs)
    arr2 = xr_da(df_aligned, df1.columns[0], coords=coords, attrs=attrs)
    assert arr1.equals(arr2)  # column specification
    assert list(arr1.coords) == df1.index.names
    arr = xr_da(df_aligned, 0, coords=coords, attrs=attrs)
    assert arr.attrs == {}

    df_aligned, coords, attrs = xr_metadata(df2)
    arr = xr_da(df_aligned, 0, coords=coords, attrs=attrs)
    assert set(df2.index.names) - set(arr.coords) == {"unit"}
    assert "unit" in arr.attrs

    df3 = df2.assign(foo=3)
    df_aligned, coords, attrs = xr_metadata(df3)
    arr1 = xr_da(df_aligned, 1, coords=coords, attrs=attrs)
    arr2 = xr_da(df_aligned, "foo", coords=coords, attrs=attrs)
    # assert arr1.name == arr2.name == "foo"
    arr1.data = arr2.data = np.where(np.isnan(arr1.data), 3, arr1.data)
    expected = np.full_like(arr1.data, 3)
    assert (arr1.data == expected).all() and (arr2.data == expected).all()


def test_to_da(pkg_w_alias):
    # alias, unit
    res = pkg_w_alias.resources[1]  # "unit" is excluded from dims
    assert len(to_da(res).dims) == glom(res, ("schema.primaryKey", len)) - 1

    res["path"] = res["path"] + ".bad"
    assert to_da(res, noexcept=True).data == None  # cannot do `is None`

    # multicol
    entry = dwim_file("testing/files/xr/index.yaml")[0]
    res = res_from_entry(entry, "testing/files/xr")
    with pytest.raises(ValueError, match="only 1 column supported"):
        to_da(res)


def test_to_dst(pkg_w_alias):
    # alias, unit
    res = pkg_w_alias.resources[1]  # "unit" is excluded from dims
    assert len(to_dst(res).dims) == glom(res, ("schema.primaryKey", len)) - 1

    res["path"] = res["path"] + ".bad"
    assert not to_dst(res, noexcept=True).data_vars

    # multicol
    entry = dwim_file("testing/files/xr/index.yaml")[0]
    res = res_from_entry(entry, "testing/files/xr")
    assert len(to_dst(res).data_vars) == 2


def test_to_mfdst(pkg_w_alias):
    dst = to_mfdst(pkg_w_alias.resources)
    assert len(dst.data_vars) == 2


@pytest.mark.skip
def test_dst_to_pkg(tmp_path, pkg_w_alias):
    pass
