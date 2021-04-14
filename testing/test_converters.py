from glom import Assign, glom
import numpy as np
import pandas as pd
import pytest

from friendly_data.converters import to_df, _schema, _source_type
from friendly_data.dpkg import pkg_from_index

from .conftest import expected_schema


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
    with pytest.warns(RuntimeWarning, match=".+: not in registry"):
        _, pkg, __ = pkg_from_index(pkg_meta, "testing/files/skip_test/index.yaml")
    df = to_df(pkg["resources"][0])
    expected = ["UK", "Ireland", "France"]
    np.testing.assert_array_equal(df.columns, expected)
    assert isinstance(df.index, pd.DatetimeIndex)


def test_pkg_to_df_aliased_cols(pkg_meta):
    _, pkg, __ = pkg_from_index(pkg_meta, "testing/files/alias_test/index.yaml")
    df = to_df(pkg["resources"][0])
    assert "resource_area" in df.columns
