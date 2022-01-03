"""Functions useful to read a data package resource into common analysis
frameworks like ``pandas``, ``xarray``, etc.  Currently supported:

=============================  ======================================
   Library                        Data Structure
=============================  ======================================
 ``pandas``                     :class:``pandas.DataFrame``
 ``xarray`` (via ``pandas``)    :class:``xarray.DataArray``,
                                :class:``xarray.Dataset``,
                                multi-file :class:``xarray.Dataset``
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
from typing import Callable, cast, Dict, Hashable, Iterable, List, Tuple, Union

from frictionless import Resource
from glom import glom, Iter, T
import pandas as pd
import xarray as xr

from friendly_data._types import _path_t, _dfseries_t
from friendly_data.dpkg import resource_
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
    remap_types = lambda t: (t["name"], type_map[t["type"]])  # noqa: E731
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
    # 0-indexed (see FIXME in `resource_`)
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


def xr_metadata(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict, Dict]:
    """Extract metadata to create xarray data array/datasets

    All indices except unit is extracted as coordinates, and "unit" is
    extracted as metadata attribute.

    Parameters
    ----------
    df : pandas.DataFrame

    Returns
    -------
    Tuple[pandas.DataFrame, Dict, Dict]
        The dataframe with units removed, dictionary of coordinates, dictionary
        with constant attributes like units

    """
    const = ["unit"]  # remove if you need multiple unit support for one table
    if isinstance(df.index, pd.MultiIndex):
        names = df.index.names
        levels = df.index.levels
    else:
        names = [df.index.name]
        levels = [df.index]
    coords = {name: lvls for name, lvls in zip(names, levels) if name not in const}
    attrs = {name: lvls[0] for name, lvls in zip(names, levels) if name in const}
    idx_aligned = pd.MultiIndex.from_product(coords.values())
    if const[0] in df.index.names:  # FIXME: resolve items in const set in index
        df = df.reset_index(const, drop=True)
    df = df.reindex(idx_aligned)
    return df, coords, attrs


def xr_da(
    df: pd.DataFrame,
    col: Union[int, Hashable],
    *,
    coords: Dict,
    attrs: Dict = {},
    **kwargs,
) -> xr.DataArray:
    """Create an xarray data array from a data frame

    Parameters
    ----------
    df : pandas.DataFrame
    col : Union[int, Hashable]
        Column to use to create the data array, either use the column number,
        or column name
    coords : Dict
        Dictionary of coordinate arrays
    attrs : Dict
        Dictionary of metadata attributes like unit

    Returns
    -------
    xarray.DataArray

    """
    indexer = getattr(df, "iloc" if isinstance(col, int) else "loc")
    arr = indexer[:, col].values
    if isinstance(arr, pd.api.extensions.ExtensionArray):
        arr = arr.to_numpy()
    data = arr.reshape(tuple(map(len, coords.values())))
    return xr.DataArray(
        data=data, coords=coords, dims=coords.keys(), attrs=attrs, **kwargs
    )


def to_da(resource: Resource, noexcept: bool = False, **kwargs) -> xr.DataArray:
    """Reads a data package resource as an :class:`xarray.DataArray`

    This function is restricted to tables with only one value column
    (equivalent to a `pandas.Series`).  All indices are treated as
    :class:`xarray.core.coordinates.DataArrayCoordinates` and dimensions.  The
    array is reshaped to match the dimensions.  Any unit index is extracted and
    attached as an attribute to the data array.  It is assumed that the whole
    table uses the same unit.

    Additional keyword arguments are passed on to :class:`xarray.DataArray`.

    Parameters
    ----------
    resource : frictionless.Resource
        List of data package resource objects
    noexcept : bool (default: False)
        Whether to suppress an exception
    **kwargs
        Additional keyword arguments that are passed on to
        :class:`xarray.DataArray`

    See Also
    --------
    :func:`to_df` : see for details on ``noexcept``

    """
    df = to_df(resource, noexcept)
    if df.empty and noexcept:
        return xr.DataArray(data=None)
    df, coords, attrs = xr_metadata(df)
    if len(df.columns) > 1:
        raise ValueError(f"{df.columns}: only 1 column supported")
    return xr_da(df, 0, coords=coords, attrs=attrs, **kwargs)


def to_dst(resource: Resource, noexcept: bool = False, **kwargs) -> xr.Dataset:
    """Reads a data package resource as an :class:`xarray.Dataset`

    Unlike :func:`to_da`, this function works for all tables.  All indices are
    treated as :class:`xarray.core.coordinates.DataArrayCoordinates` and
    dimensions.  The arrays is reshaped to match the dimensions.  Any unit
    index is extracted and attached as an attribute to each data arrays.  It is
    assumed that all columns in the whole table uses the same unit.

    Additional keyword arguments are passed on to :class:`xarray.Dataset`.

    Parameters
    ----------
    resource : frictionless.Resource
        List of data package resource objects
    noexcept : bool (default: False)
        Whether to suppress an exception
    **kwargs
        Additional keyword arguments that are passed on to
        :class:`xarray.Dataset`

    See Also
    --------
    :func:`to_df` : see for details on ``noexcept``

    """
    df = to_df(resource, noexcept)
    if df.empty and noexcept:
        return xr.Dataset()
    df, coords, attrs = xr_metadata(df)
    data_vars = {col: xr_da(df, col, coords=coords, attrs=attrs) for col in df.columns}
    return xr.Dataset(data_vars=data_vars, **kwargs)


def to_mfdst(
    resources: Iterable[Resource], noexcept: bool = False, **kwargs
) -> xr.Dataset:
    """Reads a list of data package resources as an :class:`xarray.Dataset`

    This function reads multiple resources/files and converts each column into
    a data array (identical to :func:`to_dst`), which are then combined into
    one :class:`xarray.Dataset`.  Note that any value column that is present
    more than once in the data package is overwritten by the last one.  If you
    want support for duplicates, you should use :func:`to_dst` and handle the
    duplicates yourself.

    Parameters
    ----------
    resources : List[frictionless.Resource]
        List of data package resource objects
    noexcept : bool (default: False)
        Whether to suppress an exception
    **kwargs
        Additional keyword arguments that are passed on to
        :class:`xarray.Dataset`

    See Also
    --------
    :func:`to_df` : see for details on ``noexcept``

    """
    data_vars: Dict[Hashable, xr.DataArray] = {}
    for res in resources:
        df = to_df(res, noexcept)
        if df.empty and noexcept:
            continue
        df, coords, attrs = xr_metadata(df)
        data_vars.update(
            (col, xr_da(df, col, coords=coords, attrs=attrs)) for col in df.columns
        )
    return xr.Dataset(data_vars=data_vars, **kwargs)


def resolve_aliases(df: _dfseries_t, alias: Dict[str, str]) -> _dfseries_t:
    """Return a copy of the dataframe with aliases resolved

    Parameters
    ----------
    df : pd.DataFrame | pd.Series

    alias : Dict[str, str]
        A dictionary of column aliases if the dataframe has custom column names
        that need to be mapped to columns in the registry.  The key is the
        column name in the dataframe, and the value is a column in the
        registry.

    Returns
    -------
    pd.DataFrame | pd.Series
        Since the column and index levels are renamed, a copy is returned so
        that the original dataframe/series remains unaltered.

    """
    # work w/ a copy, not very memory efficient
    _df = cast(_dfseries_t, df.rename(alias, axis=1))  # noop for pd.series
    _df.index = _df.index.rename(alias)
    return _df


def from_df(
    df: _dfseries_t,
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
    df : pd.DataFrame | pd.Series
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
    _df = resolve_aliases(df, alias) if rename else df
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
    return resource_(spec, basepath=basepath)


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
