from pathlib import Path

from glom import Assign, glom, T
import numpy as np
import pandas as pd
import pytest

from sark.dpkg import (
    create_pkg,
    read_pkg,
    to_df,
    update_pkg,
    write_pkg,
    _schema,
    _source_type,
)
from sark.helpers import select
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


def test_pkg_read(pkgdir):
    dpkg_json = pkgdir / "datapackage.json"
    pkg = read_pkg(dpkg_json)
    assert all(Path(res.source).exists() for res in pkg.resources)


def test_zippkg_read(pkg, tmp_path_factory, subtests):
    with tmp_path_factory.mktemp("ziptest-") as tmpdir:
        zipfile = tmpdir / "testpackage.zip"
        pkg.save(f"{zipfile}")

        testname = f"{zipfile} -> {zipfile.parent}"
        with subtests.test(msg="extract zip in current dir", name=testname):
            pkg = read_pkg(zipfile)
            assert pkg.valid

        subdir = zipfile.parent / "foo"
        testname = f"{zipfile} -> {subdir}"
        with subtests.test(msg="extract zip in different dir", name=testname):
            pkg2 = read_pkg(zipfile, extract_dir=subdir)
            assert pkg2.valid

        with subtests.test(msg="unsupported archive", name="tarball"):
            tarball = tmpdir / "testpackage.tar"
            with pytest.raises(ValueError, match=f".*{tarball.name}:.+"):
                read_pkg(tarball)


def test_pkg_update(pkg, subtests):
    with subtests.test(msg="schema field update", name="single"):
        resource_name = "sample-ok-1"
        update = {
            "time": {"name": "time", "type": "string", "format": "default"}
        }
        assert update_pkg(pkg, resource_name, update)
        res, *_ = glom(
            pkg.descriptor,
            (
                "resources",
                [select(T["name"], equal_to=resource_name)],
                "0.schema.fields",
                [select(T["name"], equal_to="time")],
            ),
        )
        assert update["time"] == res

    with subtests.test(msg="schema field update", name="multiple"):
        update = {
            "time": {"name": "time", "type": "datetime", "format": "default"},
            "QWE": {"name": "QWE", "type": "string", "format": "default"},
        }
        assert update_pkg(pkg, resource_name, update)
        res = glom(
            pkg.descriptor,
            (
                "resources",
                [select(T["name"], equal_to=resource_name)],
                "0.schema.fields",
                [select(T["name"], one_of=update.keys())],
            ),
        )
        assert list(update.values()) == res

    with subtests.test(msg="schema NA/index update"):
        resource_name = "sample-ok-2"
        update = {"primaryKey": ["lvl", "TRE", "IUY"]}
        assert update_pkg(pkg, resource_name, update, fields=False)
        res = glom(
            pkg.descriptor,
            (
                "resources",
                [select(T["name"], equal_to=resource_name)],
                "0.schema.primaryKey",
            ),
        )
        assert update["primaryKey"] == res

    # FIXME: test assertions inside update_pkg


def test_pkg_to_df(pkg, subtests):
    for resource in pkg.resources:
        with subtests.test(msg="default resource", name=resource.name):
            # test target, don't touch this
            df = to_df(resource)
            from_impl = expected_schema(df, type_map={})
            # read from file; strings are read as `object`, remap to `string`
            raw = expected_schema(
                resource, type_map={"object": "string", "int64": "Int64"}
            )
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
        glom(resource.descriptor, Assign("schema.missingValues", ["", "sit"]))
        resource.commit()
        df = to_df(resource)
        assert df.isna().any(axis=None)

    resource = pkg.resources[0]
    update = {
        "path": resource.descriptor["path"].replace("csv", "txt"),
        "mediatype": resource.descriptor["mediatype"].replace("csv", "plain"),
    }
    resource.descriptor.update(update)
    resource.commit()
    with subtests.test(msg="unsupported resource type", name=resource.name):
        # default behaviour
        with pytest.raises(ValueError, match="unsupported source.+"):
            df = to_df(resource)

        # suppress exceptions
        assert to_df(resource, noexcept=True).empty


def test_pkg_write(pkg, tmp_path_factory, subtests):
    with tmp_path_factory.mktemp("pkgwrite-") as tmpdir:
        zipfile = tmpdir / "testpkg.zip"

        with subtests.test(msg="save as zip", name=f"{zipfile}"):
            write_pkg(pkg, f"{zipfile}")
            assert zipfile.exists()

        tarfile = tmpdir / "testpkg.tar"
        with subtests.test(msg="unsupported archive", name=f"{tarfile}"):
            with pytest.raises(ValueError, match=f".*{tarfile.name}:.+"):
                write_pkg(pkg, f"{tarfile}")
