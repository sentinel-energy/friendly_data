from glom import Assign, glom
import numpy as np
import pytest

from sark.converters import to_df, _schema, _source_type

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
        # impl marks columns as timestamps based on the schema.  similarly
        # as per the schema, remap timestamp columns as timestamps
        ts_cols = [
            field.name for field in resource.schema.fields if "datetime" in field.type
        ]
        raw.update((col, "datetime64[ns]") for col in ts_cols)
        print(resource.name)
        assert from_impl == raw

        if not ts_cols:  # no timestamps, skip
            continue

        # resource w/ timestamps
        dtype_cmp = df[ts_cols].dtypes == np.dtype("datetime64[ns]")
        assert dtype_cmp.all(axis=None)

    # resource w/ a index
    resource = rnd_pkg.resources[0]
    field_names = [field.name for field in resource.schema.fields]
    glom(resource.descriptor, Assign("schema.primaryKey", field_names[0]))
    resource.commit()
    df = to_df(resource)
    # compare columns
    assert list(df.columns) == field_names[1:]
    # check if the right column has been set as index
    assert df.index.name == resource.schema.fields[0].name

    # resource w/ a MultiIndex
    glom(resource.descriptor, Assign("schema.primaryKey", field_names[:2]))
    resource.commit()
    df = to_df(resource)
    # compare columns
    assert list(df.columns) == field_names[2:]
    # check if the right column has been set as index
    assert df.index.names == field_names[:2]

    # resource w/ NA
    resource = rnd_pkg.resources[1]
    # set new NA value: "sit" from "Lorem ipsum dolor sit amet consectetur
    # adipiscing", TRE - 2nd column
    glom(resource.descriptor, Assign("schema.missingValues", ["", "sit"]))
    resource.commit()
    df = to_df(resource)
    assert df.isna().any(axis=None)

    # unsupported resource type
    resource = rnd_pkg.resources[0]
    update = {
        "path": resource.descriptor["path"].replace("csv", "txt"),
        "mediatype": resource.descriptor["mediatype"].replace("csv", "plain"),
    }
    resource.descriptor.update(update)
    resource.commit()
    with pytest.raises(ValueError, match="unsupported source.+"):  # default behaviour
        df = to_df(resource)
    assert to_df(resource, noexcept=True).empty  # suppress exceptions
