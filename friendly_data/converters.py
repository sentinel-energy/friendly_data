"""Functions useful to read a data package resource into common analysis
frameworks like ``pandas``, ``xarray``, etc.  Currently supported:

=============================  ======================================
   Library                        Data Structure
=============================  ======================================
 ``pandas``                     :class:``pandas.DataFrame``
 ``xarray`` (via ``pandas``)    :class:``xarray.DataArray``,
                                :class:``xarray.Dataset``,
                                multi-file :class:``xarray.Dataset``
  ``pyam`` (IAMC)		*soon*
=============================  ======================================

Type mapping between the frictionless specification and pandas types:

=============  =================
 schema type    ``pandas`` type
=============  =================
``boolean``     ``bool``
``datetime``    ``datetime64``
``integer``     ``Int64``
``number``      ``float``
``string``      ``string``
=============  =================

"""

from logging import getLogger, warn
from pathlib import Path
from typing import Callable, cast, Dict, Iterable, List, Union

from frictionless import Resource
from glom import glom, Iter, T
import pandas as pd
import xarray as xr

from friendly_data._types import _path_t, _dfseries_t
from friendly_data.dpkg import _resource
from friendly_data.dpkg import fullpath
from friendly_data.dpkg import get_aliased_cols
from friendly_data.dpkg import index_levels
from friendly_data.helpers import consume
from friendly_data.helpers import import_from
from friendly_data.helpers import noop_map
from friendly_data.helpers import sanitise

logger = getLogger(__name__)

# TODO: compressed files
_pd_types = {
    "boolean": "bool",
    "date": "datetime64",
    "time": "datetime64",
    "datetime": "datetime64",
    "year": "Int64",
    "yearmonth": "datetime64",
    "integer": "Int64",
    "number": "float",
    "string": "string",
}
_pd_readers = {
    "csv": "read_csv",
    "xls": "read_excel",
    "xlsx": "read_excel",
    # "sqlite": "read_sql",
}


def _source_type(source: _path_t) -> str:
    """From a file path, deduce the file type from the extension

    Note: the extension is checked against the list of supported file types

    """
    # FIXME: use file magic
    source_t = Path(source).suffix.strip(".").lower()
    if source_t not in _pd_readers:
        raise ValueError(f"unsupported source: {source}")
    return source_t


def _reader(fpath, **kwargs) -> _dfseries_t:
    reader = cast(Callable, import_from("pandas", _pd_readers[_source_type(fpath)]))
    return reader(fpath, **kwargs)


def _schema(resource: Resource, type_map: Dict[str, str]) -> Dict[str, str]:
    """Parse a Resource schema and return types mapped to each column.

    Parameters
    ----------
    resource : frictionless.Resource
        A resource descriptor
    type_map : Dict[str, str]
        A dictionary that maps datapackage type names to pandas types.

    Returns
    -------
    Dict[str, str]
        Dictionary with column names as key, and types as values

    """
    remap_types = lambda t: (t["name"], type_map[t["type"]])
    return glom(resource, ("schema.fields", [remap_types], dict))


def to_df(resource: Resource, noexcept: bool = False, **kwargs) -> pd.DataFrame:
    """Reads a data package resource as a `pandas.DataFrame`

    FIXME: 'format' in the schema is ignored.

    Parameters
    ----------
    resource : frictionless.Resource
        A data package resource object
    noexcept : bool (default: False)
        Whether to suppress an exception
    **kwargs
        Additional keyword arguments that are passed on to the reader:
        :func:`pandas.read_csv`, :func:`pandas.read_excel`, etc

    Returns
    -------
    pandas.DataFrame
        NOTE: when ``noexcept`` is ``True``, and there's an exception, an empty
        dataframe is returned

    Raises
    ------
    ValueError
        If the resource is not local
        If the source type the resource is pointing to isn't supported

    """
    from pandas._libs.parsers import STR_NA_VALUES

    # parse dates
    schema = _schema(resource, _pd_types)
    date_cols = [col for col, col_t in schema.items() if "datetime64" in col_t]
    consume(map(schema.pop, date_cols))

    # missing values, NOTE: pandas accepts a list of "additional" tokens to be
    # treated as missing values.
    na_values = (
        glom(resource, ("schema.missingValues", set), default=set()) - STR_NA_VALUES
    )
    # FIXME: check if empty set is the same as None

    # FIXME: how to handle constraints? e.g. 'required', 'unique', 'enum', etc
    # see: https://specs.frictionlessdata.io/table-schema/#constraints

    # set 'primaryKey' as index_col, a list is interpreted as a MultiIndex
    index_col = glom(resource, ("schema.primaryKey"), default=False)
    if isinstance(index_col, list):
        # guard against schema, that includes an index column
        [schema.pop(col) for col in index_col if col in schema]

    # FIXME: skip_rows is 1-indexed, whereas skiprows is either an offset or
    # 0-indexed (see FIXME in `_resource`)
    skiprows = glom(resource, ("layout.skipRows", len), default=None)

    # don't let the user override the options we use
    [
        kwargs.pop(k, None)
        for k in ("dtype", "na_values", "index_col", "parse_dates", "skiprows")
    ]

    alias = glom(
        resource,
        (
            "schema.fields",
            Iter()
            .filter(lambda i: "alias" in i)
            .map(({1: "name", 2: "alias"}, T.values()))
            .all(),
            noop_map,
        ),
    )
    try:
        # FIXME: validate options
        df = _reader(
            fullpath(resource),
            dtype=schema,
            na_values=na_values,
            index_col=index_col,
            parse_dates=date_cols,
            skiprows=skiprows,
            **kwargs,
        ).rename(columns=alias)
    except ValueError:
        if noexcept:
            return pd.DataFrame()
        else:
            raise
    else:
        if isinstance(df.index, pd.MultiIndex):
            df.index.names = [alias[n] for n in df.index.names]
        else:
            df.index.name = alias[df.index.name]
        return df


def to_da(resource: Resource, noexcept: bool = False, **kwargs) -> xr.DataArray:
    """Reads a data package resource as an `xarray.DataArray`

    Additional keyword arguments are passed on to :class:`xarray.Dataset`.  See
    :func:`to_df` for more details on the other arguments.

    """
    df = to_df(resource, noexcept)
    return xr.DataArray(df, **kwargs)


def to_dst(resource: Resource, noexcept: bool = False, **kwargs) -> xr.Dataset:
    """Reads a data package resource as an `xarray.Dataset`

    Additional keyword arguments are passed on to :class:`xarray.Dataset`.  See
    :func:`to_df` for more details on the other arguments.

    """
    df = to_df(resource, noexcept)
    return xr.Dataset({resource["name"]: df}, **kwargs)


def to_mfdst(
    resources: Iterable[Resource], noexcept: bool = False, **kwargs
) -> xr.Dataset:
    """Reads a list of data package resources as an `xarray.DataArray`

    See :func:`to_df` for more details.

    Parameters
    ----------
    resources : List[frictionless.Resource]
        List of data package resource objects
    noexcept : bool (default: False)
        Whether to suppress an exception
    **kwargs
        Additional keyword arguments that are passed on to
        :class:`xarray.Dataset`

    """
    dfs = {res["name"]: to_df(res, noexcept) for res in resources}
    return xr.Dataset(dfs, **kwargs)


def from_df(
    df: Union[pd.DataFrame, pd.Series],
    basepath: _path_t,
    datapath: _path_t = "",
    alias: Dict[str, str] = {},
    rename: bool = True,
) -> Resource:
    """Write dataframe to a CSV file, and return a data package resource.

    NOTE: Do not call ``frictionless.Resource.infer()`` on the resource
    instance returned by this function, as that might overwrite our
    metadata/schema customisations with default heuristics in the
    ``frictionless`` implementation.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to write

    basepath : Union[str, Path]
        Path to the package directory

    datapath : Union[str, Path] (default: empty string)
        Path to the CSV file where the dataframe is written.  If `datapath` is
        empty, a file name is generated by concatinating all the columns in the
        dataframe.

    alias : Dict[str, str] (default: {})
        A dictionary of column aliases if the dataframe has custom column names
        that need to be mapped to columns in the registry.  The key is the
        column name in the dataframe, and the value is a column in the
        registry.

    rename : bool (default: True)
        Rename aliased columns to match the registry when writing to the CSV.

    Returns
    -------
    frictionless.Resource
        Data package resource that points to the CSV file.

    """
    if not datapath:
        if isinstance(df, pd.Series):
            datapath = f"{df.name}.csv"
        else:
            datapath = f"{'_'.join(sanitise(col) for col in df.columns)}.csv"
    fullpath = Path(basepath) / datapath
    # ensure parent directory exists
    fullpath.parent.mkdir(parents=True, exist_ok=True)
    if rename:
        # work w/ a copy, not very memory efficient
        _df = df.rename(alias, axis=1)  # noop for pd.series
        _df.index = _df.index.rename(alias)
    else:
        _df = df
    # don't write index if default/unnamed index
    defaultidx = (
        False if isinstance(_df.index, pd.MultiIndex) else _df.index.name is None
    )
    _df.to_csv(fullpath, index=not defaultidx)

    cols = [_df.name] if isinstance(_df, pd.Series) else _df.columns
    coldict = get_aliased_cols(cols, "cols", {} if rename else alias)
    if not defaultidx:
        idxcols = (
            _df.index.names
            if isinstance(_df.index, pd.MultiIndex)
            else [_df.index.name]
        )
        if None in idxcols:
            warn(f"index doesn't have valid names: {idxcols}")
        _, idxcoldict = index_levels(_df, idxcols, alias)
    else:
        idxcols = []
        idxcoldict = {}
    spec = {
        "path": f"{datapath}",
        "schema": {"fields": {**idxcoldict, **coldict}},
    }
    if not defaultidx:
        spec["schema"]["primaryKey"] = list(idxcols)  # type: ignore[index]
    return _resource(spec, basepath=basepath)


def from_dst(
    dst: xr.Dataset,
    basepath: _path_t,
    alias: Dict[str, str] = {},
) -> List[Resource]:
    """Write an ``xarray.Dataset`` into CSV files, and return the list resources

    Each data variable is written to a separate CSV file in the directory
    specified by `basepath`.  The file name is derived from the data variable
    name by sanitising it and appending the CSV extension.

    Parameters
    ----------
    dst : xr.Dataset
        Dataset to write

    basepath : Union[str, Path]
        Path to the package directory

    alias : Dict[str, str]
        A dictionary of column aliases if the dataset has custom data
        variable/coordinate names that need to be mapped to columns in the
        registry.

    Returns
    -------
    List[Resource]
        List of data package resources that point to the CSV files.

    """
    resources = [
        from_df(
            da.to_dataframe().dropna(),
            basepath,
            datapath=f"{sanitise(var)}.csv",  # type: ignore[arg-type]
            alias=alias,
        )
        for var, da in dst.data_vars.items()
    ]
    return resources
