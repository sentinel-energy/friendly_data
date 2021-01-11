"""Datapackage"""
# PS: the coincidential module name is intentional ;)

import csv
from itertools import chain
import json
from pathlib import Path
from typing import Dict, Iterable, Optional, TextIO, Tuple, Union
from warnings import warn
from zipfile import ZipFile

from datapackage import Package, Resource
from glom import Assign, glom, Invoke, Iter, Spec, T
import pandas as pd
from pkg_resources import resource_filename
import yaml

from sark.helpers import consume, import_from, match, select

# TODO: compressed files
_source_ts = ["csv", "xls", "xlsx"]  # "sqlite"
_pd_types = {
    "boolean": "bool",
    "date": "datetime64",
    "time": "datetime64",
    "datetime": "datetime64",
    "year": "datetime64",
    "yearmonth": "datetime64",
    "integer": "Int64",
    "number": "float",
    "string": "string",
}

_path_t = Union[str, Path]  # file path type


def create_pkg(meta: Dict, resources: Iterable[_path_t], base_path: _path_t = ""):
    """Create a datapackage from metadata and resources.

    If `resources` point to files that exist, their schema are inferred and
    added to the package.  If `base_path` is a non empty string, it is treated
    as the parent directory, and all resource file paths are checked relative
    to it.

    Parameters
    ----------
    meta : Dict
        A dictionary with package metadata.

    resources : Iterable[Union[str, Path]]
        An iterator over different resources.  Resources are paths to files,
        relative to `base_path`.

    base_path : str (default: empty string)
        Directory where the package files are located

    Returns
    -------
    Package
        A datapackage with inferred schema for all the package resources

    """
    # for an interesting discussion on type hints with unions, see:
    # https://stackoverflow.com/q/60235477/289784
    pkg = Package(meta, base_path=str(base_path))
    # TODO: filter out and handle non-tabular (custom) data
    for res in resources:
        if isinstance(res, (str, Path)):
            full_path = Path(base_path) / res
            if not full_path.exists():
                warn(f"{full_path}: skipped, doesn't exist", RuntimeWarning)
                continue
            pkg.infer(f"{res}")
        else:  # pragma: no cover, adding with Dict (undocumented feature)
            pkg.add_resource(res)
    return pkg


def read_pkg(pkg_path: _path_t, extract_dir: Optional[_path_t] = None):
    """Read a  datapackage

    If `pkg_path` points to a `datapackage.json` file, read it as is.  If it
    points to a zip archive.  The archive is first extracted before opening it.
    If `extract_dir` is not provided, the current directory of the zip archive
    is used.

    Parameters
    ----------
    pkg_path : Union[str, Path]
        Path to the `datapackage.json` file, or a zip archive

    extract_dir : Union[str, Path]
        Path to which the zip archive is extracted

    Returns
    -------
    Package

    """
    pkg_path = Path(pkg_path)
    if pkg_path.suffix == ".json":
        with open(pkg_path) as pkg_json:
            base_path = f"{Path(pkg_path).parent}"
            return Package(json.load(pkg_json), base_path=base_path)
    elif pkg_path.suffix == ".zip":
        if extract_dir is None:
            extract_dir = pkg_path.parent
        else:
            extract_dir = Path(extract_dir)
        with ZipFile(pkg_path) as pkg_zip:
            pkg_zip.extractall(path=extract_dir)
            with open(extract_dir / "datapackage.json") as pkg_json:
                return Package(json.load(pkg_json), base_path=f"{extract_dir}")
    else:
        raise ValueError(f"{pkg_path}: expecting a JSON or ZIP file")


def update_pkg(pkg, resource, schema_update: Dict, fields: bool = True):
    """Update package resource schema

    Parameters
    ----------
    pkg
        Package object

    resource
        Resource name FIXME: cannot handle duplicate names in subdirectories

    schema_update : Dict
        Updated fields in the schema, if `field` is `False`, can be used to
        update `missingValues`, or `primaryKey`.  When updating the schema, it
        looks like this ('foo'/'bar' are names of the fields being updated)::

          {
              "foo": {
                  "name": "foo",
                  "type": "datetime",
                  "format": "default"
              },
              "bar": {
                  "name": "bar",
                  "type": "integer",
                  "format": "default"
              }
          }

    fields : bool (default: True)
        If the schema update is a field, or not

    Returns
    -------
    bool
        Return the `Package.valid` flag; `True` if the update was valid.

    """
    desc = pkg.descriptor
    assert "resources" in desc, "Package should have at least one resource"
    res, *_ = [res for res in desc["resources"] if res["name"] == resource]
    if fields:
        for field in res["schema"]["fields"]:
            if field["name"] in schema_update:
                field.update(schema_update[field["name"]])
    else:
        # FIXME: do the following checks properly
        assert "fields" not in schema_update, "cannot add fields to schema"
        # prevents from adding additional keys
        assert set(schema_update) - {"primaryKey", "missingValues"} == set()
        res["schema"].update(schema_update)
    pkg.commit()
    return pkg.valid


def read_pkg_index(fpath: Union[_path_t, TextIO], suffix: str = "") -> pd.DataFrame:
    """Read the index of files incuded in the datapackage

    The index can be in either CSV, YAML, or JSON file format.  It is a list of
    dataset files, names, and a list of columns in the dataset that are to be
    treated as index columns.

    CSV::

        >>> csv_f = '''
        ... file,name,idxcols
        ... file1,dst1,cola,colb
        ... file2,dst2,colx,coly,colz
        ... file3,dst3,col
        ... '''

    YAML::

        >>> yaml_f = '''
        ... - file: file1
        ...   name: dst1
        ...   idxcols: [cola, colb]
        ... - file: file2
        ...   name: dst2
        ...   idxcols: [colx, coly, colz]
        ... - file: file3
        ...   name: dst3
        ...   idxcols: [col]
        ... '''

    JSON::

        >>> json_f = '''
        ... [
        ...     {
        ...         "file": "file1",
        ...         "name": "dst1",
        ...         "idxcols": [
        ...             "cola",
        ...             "colb"
        ...         ]
        ...     },
        ...     {
        ...         "file": "file2",
        ...         "name": "dst2",
        ...         "idxcols": [
        ...             "colx",
        ...             "coly",
        ...             "colz"
        ...         ]
        ...     },
        ...     {
        ...         "file": "file3",
        ...         "name": "dst3",
        ...         "idxcols": [
        ...             "col"
        ...         ]
        ...     }
        ... ]
        ... '''

    Parameters
    ----------
    fpath : Union[str, Path, TextIO]
        Index file path or a stream object

    suffix : str (default: empty string)
        File type, one of: csv, yaml, yml, json.  If it is empty (default), the
        file type is deduced from the filename extension.  Since a stream does
        not always have a file associated with it, it is mandatory to specify a
        non-empty `suffix` when `fpath` is a stream.

    Returns
    -------
    pd.DataFrame
        A dataframe with the columns: 'file', 'name', and 'idxcols'; 'idxcols'
        is a tuple.

    Raises
    ------
    ValueError
        If 'suffix' is empty when 'fpath' is a stream, or it does not have an
        extension.
    RuntimeError
        If the file is a YAML, or JSON, but do not return a list
        If the file has an unknown extension

    Examples
    --------

    Index as read from the example above::

        >>> from io import StringIO
        >>> import numpy as np
        >>> idx = read_pkg_index(StringIO(csv_f), 'csv')
        >>> idx
             file  name             idxcols
        0   file1  dst1        (cola, colb)
        1   file2  dst2  (colx, coly, colz)
        2   file3  dst3              (col,)
        >>> np.array_equal(idx, read_pkg_index(StringIO(yaml_f), 'yaml'))
        True
        >>> np.array_equal(idx, read_pkg_index(StringIO(yaml_f), 'yml'))
        True
        >>> np.array_equal(idx, read_pkg_index(StringIO(json_f), 'json'))
        True

    """
    if isinstance(fpath, (str, Path)):
        idxfile = open(fpath)
        suffix = suffix if suffix else Path(fpath).suffix.strip(".").lower()
    else:  # stream
        idxfile = fpath
        suffix = suffix.lower()
    if not suffix:
        raise ValueError(f"suffix={suffix} cannot be empty, when fpath={fpath}")

    if suffix == "csv":
        idx = [
            {"file": fname, "name": dst, "idxcols": idxcols}
            for fname, dst, *idxcols in csv.reader(idxfile)
        ]
        idx = idx[1:]  # drop header row
    elif suffix in ("yaml", "yml"):
        idx = yaml.safe_load(idxfile)
    elif suffix == "json":
        idx = json.load(idxfile)
    else:
        idxfile.close()  # cleanup
        raise RuntimeError(f"{fpath}: unknown index file format")

    idxfile.close()  # cleanup

    if not isinstance(idx, list):
        raise RuntimeError(f"{fpath}: bad index file")

    # # convert list (idxcols) into tuples, easier to query in DataFrame
    # glom(idx, [Assign("idxcols", Spec((T["idxcols"], tuple)))])
    return pd.DataFrame(idx)


# FIXME: can't use Literal until we drop 3.7
def registry(col: str, col_t: str) -> Dict:
    """Retrieve the column schema from column schema registry: `sark_registry`

    Parameters
    ----------
    col : str
        Column name to look for

    col_t : Literal["cols", "idxcols"]
        A literal string specifying the kind of column; one of: "cols", or "idxcols"

    Returns
    -------
    Dict
        Column schema; an empty dictionary is returned in case there are no matches

    Raises
    ------
    RuntimeError
        When more than one matches are found
    ValueError
        When the schema file in the registry is unsupported; not one of: JSON, or YAML

    """
    if col_t not in ("cols", "idxcols"):
        raise ValueError(f"{col_t}: unknown column type")

    curdir = Path(resource_filename("sark_registry", col_t))
    schema = list(
        chain.from_iterable(curdir.glob(f"{col}.{fmt}") for fmt in ("json", "yaml"))
    )
    if len(schema) == 0:
        warn(f"{col}: not in registry", RuntimeWarning)
        return {}  # no match, unregistered column
    if len(schema) > 1:  # pragma: no cover, bad registry
        raise RuntimeError(f"{schema}: multiple matches, duplicates in registry")
    with open(curdir / schema[0]) as f:
        fsuffix = Path(f.name).suffix.strip(".").lower()
        if fsuffix == "yaml":
            return yaml.safe_load(f)
        elif fsuffix == "json":
            return json.load(f)
        else:  # pragma: no cover, shouldn't reach here
            raise ValueError(f"{f.name}: unsupported schema file format")


def index_levels(_file: _path_t, idxcols: Iterable[str]) -> Tuple[_path_t, Dict]:
    """Read a dataset and determine the index levels

    Parameters
    ----------
    _file : str
        Path to the dataset

    idxcols : Iterable[str]
        List of columns in the dataset that constitute the index

    Returns
    -------
    Tuple[str, Dict]

        Tuple of path to the dataset, and the schema of each column as a dictionary.
        If `idxcols` was ["foo", "bar"], the dictionary might look like::

          {
              "foo": {
                  "name": "foo",
                  "type": "datetime",
                  "format": "default"
              },
              "bar": {
                  "name": "bar",
                  "type": "string",
                  "constraints": {
                      "enum": ["a", "b"]
                  }
              }
          }

        Note that the index columns that have categorical values, are filled in
        by reading the dataset and determining the full set of values.

    """
    coldict = {col: registry(col, "idxcols") for col in idxcols}
    # select columns with an enum constraint where the enum values are empty
    select_cols = match({"constraints": {"enum": []}, str: str})
    cols = glom(coldict.values(), Iter().filter(select_cols).map("name").all())
    idx = pd.read_csv(_file, index_col=cols).index
    if isinstance(idx, pd.MultiIndex):
        levels = {col: list(lvls) for col, lvls in zip(idx.names, idx.levels)}
    else:
        levels = {idx.names[0]: list(idx.unique())}
    enum_vals = Spec(Invoke(levels.__getitem__).specs("name"))
    glom(
        coldict.values(),
        Iter().filter(select_cols).map(Assign("constraints.enum", enum_vals)).all(),
    )
    return _file, coldict


def pkg_from_index(meta: Dict, fpath: _path_t) -> Tuple[Path, Package, pd.DataFrame]:
    """Read an index file, and create a datapackage with the provided metadata.

    Parameters
    ----------
    meta : Dict
        Package metadata dictionary

    fpath : Union[str, Path]
        Path to the index file.  Note the index file has to be at the top level
        directory of the datapackage.  See :func:`sark.dpkg.read_pkg_index`

    Returns
    -------
    Tuple[Path, Package, pandas.DataFrame]
        The package directory, the `Package` object, and the index dataframe.

    """
    pkg_dir = Path(fpath).parent
    idx = read_pkg_index(fpath)
    # FIXME: also update regular columns
    pkg = create_pkg(meta, idx["file"], base_path=f"{pkg_dir}")
    for entry in idx.to_records():
        resource_name = Path(entry.file).stem
        _, update = index_levels(pkg_dir / entry.file, entry.idxcols)
        update_pkg(pkg, resource_name, update)
        update_pkg(pkg, resource_name, {"primaryKey": entry.idxcols}, fields=False)
        # set of value columns
        cols = (
            glom(
                pkg.descriptor,
                (
                    "resources",
                    Iter()
                    .filter(select("path", equal_to=entry.file))
                    .map("schema.fields")
                    .flatten()
                    .map("name")
                    .all(),
                    set,
                ),
            )
            - set(entry.idxcols)
        )
        update_pkg(pkg, resource_name, {col: registry(col, "cols") for col in cols})
    return pkg_dir, pkg, idx


def pkg_glossary(pkg: Package, idx: pd.DataFrame) -> pd.DataFrame:
    """Generate glossary from the package and the package index.

    Parameters
    ----------
    pkg : Package

    idx : pd.DataFrame
        The index dataframe

    Returns
    -------
    pd.DataFrame
        The glossary as dataframe

    """
    _levels = lambda row: glom(
        pkg.descriptor,
        (
            "resources",
            Iter()
            .filter(select("path", equal_to=row["file"]))
            .map("schema.fields")
            .flatten()
            .filter(
                match({"name": row["idxcols"], "constraints": {"enum": list}, str: str})
            )
            .map("constraints.enum")
            .flatten()
            .all(),
        ),
    )
    glossary = idx.explode("idxcols").reset_index(drop=True)
    return glossary.assign(values=glossary.apply(_levels, axis=1))


def write_pkg(pkg, pkg_path: _path_t):
    """Write data package to a zip file

    NOTE: This exists because we want to support saving to other kinds of
    archives like tar, maybe even HDF5, or NetCDF.

    """
    pkg_path = Path(pkg_path)
    if pkg_path.suffix == ".zip":
        pkg.save(pkg_path)
    else:
        raise ValueError(f"{pkg_path}: not a zip file")


def _source_type(source: _path_t) -> str:
    """From a file path, deduce the file type from the extension

    Note: the extension is checked against the list of supported file types

    """
    # FIXME: use file magic
    source_t = Path(source).suffix.strip(".").lower()
    if source_t not in _source_ts:
        raise ValueError(f"unsupported source: {source_t}")
    return source_t


def _schema(resource: Resource, type_map: Dict[str, str]) -> Dict[str, str]:
    """Parse a Resource schema and return types mapped to each column.

    Parameters
    ----------
    resource
        A resource descriptor
    type_map : Dict[str, str]
        A dictionary that maps datapackage type names to pandas types.

    Returns
    -------
    Dict[str, str]
        Dictionary with column names as key, and types as values

    """
    return dict(
        glom(
            resource,  # target
            (  # spec
                "schema.fields",  # Resource & Schema properties
                [  # fields inside a list
                    (
                        "descriptor",  # Field property
                        # string names -> string dtypes understood by pandas
                        lambda t: (t["name"], type_map[t["type"]]),
                    )
                ],
            ),
        )
    )


def to_df(resource: Resource, noexcept: bool = False) -> pd.DataFrame:
    """Reads a data package resource as a `pandas.DataFrame`

    FIXME: only considers 'name' and 'type' in the schema, other options like
    'format', 'missingValues', etc are ignored.

    Parameters
    ----------
    resource : `datapackage.Resource`
        A data package resource object
    noexcept : bool (default: False)
        Whether to suppress an exception

    Returns
    -------
    pandas.DataFrame
        NOTE: when `noexcept` is `True`, and there's an exception, an empty
        dataframe is returned

    Raises
    ------
    ValueError
        If the resource is not local
        If the source type the resource is pointing to isn't supported

    """
    if not resource.local:  # pragma: no cover, not implemented
        if noexcept:
            return pd.DataFrame()
        else:
            raise ValueError(f"{resource.source}: not a local resource")

    pd_readers = {
        "csv": "read_csv",
        "xls": "read_excel",
        "xlsx": "read_excel",
        # "sqlite": "read_sql",
    }
    try:
        reader = import_from("pandas", pd_readers[_source_type(resource.source)])
    except ValueError:
        if noexcept:
            return pd.DataFrame()
        else:
            raise

    # parse dates
    schema = _schema(resource, _pd_types)
    date_cols = [col for col, col_t in schema.items() if "datetime64" in col_t]
    consume(map(schema.pop, date_cols))

    # missing values, NOTE: pandas accepts a list of "additional" tokens to be
    # treated as missing values.
    na_values = (
        glom(resource, ("descriptor.schema.missingValues", set))
        - pd._libs.parsers.STR_NA_VALUES
    )
    # FIXME: check if empty set is the same as None

    # FIXME: how to handle constraints? e.g. 'required', 'unique', 'enum', etc
    # see: https://specs.frictionlessdata.io/table-schema/#constraints

    # set 'primaryKey' as index_col, a list is interpreted as a MultiIndex
    index_col = glom(resource, ("descriptor.schema.primaryKey"), default=False)

    return reader(
        resource.source,
        dtype=schema,
        na_values=na_values,
        index_col=index_col,
        parse_dates=date_cols,
    )
