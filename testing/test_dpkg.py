from pathlib import Path

from datapackage import Package
from glom import Assign, glom
import numpy as np
import pandas as pd
import pytest

from sark.dpkg import create_pkg, to_df, _schema, _source_type
from sark.metatools import get_license


class noop_map:
    def __getitem__(self, key):
        return key


# values as per noop
default_type_map = {
    "object": "string",
    "float64": "number",
    "int64": "integer",
    "Int64": "integer",
    "bool": "boolean",
}


def expected_schema(df, type_map=default_type_map):
    # handle a resource and a path
    if not isinstance(df, pd.DataFrame):
        try:
            df = pd.read_csv(df.source)
        except AttributeError:
            df = pd.read_csv(df)  # path
    # datapackage.Package.infer(..) relies on tableschema.Schema.infer(..),
    # which infers datetime as string
    return (
        df.dtypes.astype(str)
        .map(lambda t: type_map[t] if t in type_map else t)
        .to_dict()
    )


def test_source_type_heuristics():
    no_file = "/path/to/non-existent-file.ext"
    with pytest.raises(ValueError):
        _source_type(no_file)


@pytest.mark.skip(reason="not sure how to test schema parsing")
def test_schema_parsing():
    pass


def test_pkg_creation(pkgdir, subtests):
    datadir = pkgdir / "data"
    pkg_meta = {"name": "test", "licenses": get_license("CC0-1.0")}
    pkg = create_pkg(pkg_meta, datadir.glob("sample-ok-?.csv"))
    for resource in pkg.resources:
        with subtests.test(msg="resource", name=resource.name):
            assert _schema(resource, noop_map()) == expected_schema(resource)


def test_pkg_read(pkg):
    assert all(Path(res.source).exists() for res in pkg.resources)


def test_pkg_to_df(pkg, tmp_path_factory, subtests):
    for resource in pkg.resources:
        with subtests.test(msg="default resource", name=resource.name):
            # test target, don't touch this
            df = to_df(resource)
            from_impl = expected_schema(df, type_map={})
            # read from file; strings are read as `object`, remap to `string`
            raw = expected_schema(resource, type_map={"object": "string"})
            # impl marks columns as timestamps based on the schema.  similarly
            # as per the schema, remap timestamp columns as timestamps
            ts_cols = [
                field.name
                for field in resource.schema.fields
                if "datetime" in field.type
            ]
            raw.update((col, "datetime64[ns]") for col in ts_cols)
            assert from_impl == raw

        if not ts_cols:  # no timestamps, skip
            continue

        with subtests.test(msg="resource with timestamps", name=resource.name):
            dtype_cmp = df[ts_cols].dtypes == np.dtype("datetime64[ns]")
            assert dtype_cmp.all(axis=None)

    resource = pkg.resources[0]
    field_names = [field.name for field in resource.schema.fields]
    with subtests.test(msg="resource with Index", name=resource.name):
        glom(resource.descriptor, Assign("schema.primaryKey", field_names[0]))
        resource.commit()
        df = to_df(resource)
        # compare columns
        assert list(df.columns) == field_names[1:]
        # check if the right column has been set as index
        assert df.index.name == resource.schema.fields[0].name

    with subtests.test(msg="resource with MultiIndex", name=resource.name):
        glom(resource.descriptor, Assign("schema.primaryKey", field_names[:2]))
        resource.commit()
        df = to_df(resource)
        # compare columns
        assert list(df.columns) == field_names[2:]
        # check if the right column has been set as index
        assert df.index.names == field_names[:2]

    resource = pkg.resources[1]
    with subtests.test(msg="resource with NA", name=resource.name):
        # set new NA value: "sit" from "Lorem ipsum dolor sit amet
        # consectetur adipiscing", TRE - 2nd column
        glom(
            resource.descriptor, Assign("schema.missingValues", ["", "sit"]),
        )
        resource.commit()
        df = to_df(resource)
        assert df.isna().any(axis=None)


@pytest.mark.skip(reason="not implmented")
def test_pkg_write(pkg, tmp_path_factory):
    with tmp_path_factory.mktemp("mutate-") as tmpdir:
        # create new datapackage
        newpkg = Package(pkg.descriptor, base_path=str(tmpdir))
        # write CSV files
        for old, new in zip(pkg.resources, newpkg.resources):
            dest = Path(new.source)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(Path(old.source).read_text())
