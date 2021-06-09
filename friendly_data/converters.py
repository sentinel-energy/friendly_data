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

from itertools import product
from logging import getLogger
from pathlib import Path
from typing import cast, Dict, Iterable, List, Union

from frictionless import Resource
from glom import glom, Iter, Invoke, Match, MatchError, T
import pandas as pd
import pyam
import xarray as xr

from friendly_data._types import _path_t
from friendly_data.dpkg import _resource
from friendly_data.dpkg import fullpath
from friendly_data.dpkg import get_aliased_cols
from friendly_data.dpkg import index_levels
from friendly_data.dpkg import pkgindex
from friendly_data.dpkg import res_from_entry
from friendly_data.helpers import consume, import_from, is_fmtstr, noop_map, sanitise
from friendly_data.io import dwim_file

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


def _reader(fpath: _path_t, **kwargs) -> Union[pd.DataFrame, pd.Series]:
    reader = import_from("pandas", _pd_readers[_source_type(fpath)])
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
        df.index.names = [alias[n] for n in df.index.names]
        return df


def to_da(resource: Resource, noexcept: bool = False, **kwargs) -> xr.DataArray:
    """Reads a data package resource as an `xarray.DataArray`

    Additional keyword arguments are passed on to :class:`xarray.Dataset`.  See
    :func:`~friendly_data.converters.to_df` for more details on the other arguments.

    """
    df = to_df(resource, noexcept)
    return xr.DataArray(df, **kwargs)


def to_dst(resource: Resource, noexcept: bool = False, **kwargs) -> xr.Dataset:
    """Reads a data package resource as an `xarray.Dataset`

    Additional keyword arguments are passed on to :class:`xarray.Dataset`.  See
    :func:`~friendly_data.converters.to_df` for more details on the other arguments.

    """
    df = to_df(resource, noexcept)
    return xr.Dataset({resource["name"]: df}, **kwargs)


def to_mfdst(
    resources: Iterable[Resource], noexcept: bool = False, **kwargs
) -> xr.Dataset:
    """Reads a list of data package resources as an `xarray.DataArray`

    See :func:`~friendly_data.converters.to_df` for more details.

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
    df: pd.DataFrame,
    basepath: _path_t,
    datapath: _path_t = "",
    alias: Dict[str, str] = {},
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

    Returns
    -------
    frictionless.Resource
        Data package resource that points to the CSV file.

    """
    if not datapath:
        datapath = f"{'_'.join(sanitise(col) for col in df.columns)}.csv"
    fullpath = Path(basepath) / datapath
    # ensure parent directory exists
    fullpath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(fullpath)

    coldict = get_aliased_cols(df.columns, "cols", alias)
    _, idxcoldict = index_levels(df, df.index.names, alias)
    spec = {
        "path": f"{datapath}",
        "schema": {
            "fields": {**idxcoldict, **coldict},
            "primaryKey": list(df.index.names),
        },
    }
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
            datapath=f"{sanitise(var)}.csv",  # type: ignore
            alias=alias,
        )
        for var, da in dst.data_vars.items()
    ]
    return resources


class IAMconv:
    """Converter class for IAMC data

    This class resolves index columns against the "semi-hierarchical" variables
    used in IAMC data, and separates them into individual datasets that are
    part of the datapackage.  It relies on the index file and index column
    definitions to do the disaggregation.  It also supports the reverse
    operation of aggregating multiple datasets into an IAMC dataset.

    TODO:
    - describe assumptions (e.g. case insensitive match) and fallbacks (e.g. missing title)
    - limitations (e.g. when no index column exists)

    """

    @classmethod
    def _validate(cls, conf: Dict) -> Dict:
        # FIXME: check if file exists
        conf_match = Match(
            {
                "indices": [str],
                str: object,  # fall through for other config keys
            }
        )
        try:
            return glom(conf, conf_match)
        except MatchError as err:
            logger.exception(
                f"{err.args[1]}: must define a list of files pointing to idxcol"
                "definitions for IAMC conversion"
            )
            raise

    @classmethod
    def _warn_empty(cls, df: pd.DataFrame, entry: Dict):
        if df.empty:
            logger.warning(f"{entry['path']}: empty dataframe, check index entry")

    @classmethod
    def iamdf2df(cls, iamdf: pyam.IamDataFrame) -> pd.DataFrame:
        """Convert :class:`pyam.IamDataFrame` to :class:`pandas.DataFrame`"""
        return (
            iamdf.as_pandas()
            .drop(columns="exclude")
            .set_index(pyam.IAMC_IDX + ["year"])
        )

    @classmethod
    def f2col(cls, fpath: _path_t) -> str:
        """Deduce column name from file name"""
        return Path(fpath).stem

    @classmethod
    def from_iamdf_simple(
        cls, iamdf: pyam.IamDataFrame, basepath: _path_t, datapath: _path_t
    ) -> Resource:
        """Simple conversion to data package in IAM long format"""
        return from_df(cls.iamdf2df(iamdf), basepath, datapath)

    @classmethod
    def from_file(cls, confpath: _path_t, idxpath: _path_t, **kwargs) -> "IAMconv":
        """Create a mapping of IAMC indicator variables with index columns

        Parameters
        ----------
        confpath : Union[str, Path]
            Path to config file for IAMC <-> data package config file

        idxpath : Union[str, Path]
            Path to index file

        **kwargs
            Keyword arguments passed on to the pandas reader backend.

        Returns
        -------
        IAMconv


        """
        conf = cls._validate(cast(Dict, dwim_file(confpath)))
        indices: Dict[str, pd.Series] = {
            cls.f2col(path): _reader(
                path,
                usecols=["name", "title"],
                index_col="name",
                squeeze=True,
                **kwargs,
            )
            for path in conf["indices"]
        }
        return cls(pkgindex.from_file(idxpath), indices, basepath=Path(idxpath).parent)

    def __init__(self, idx: pkgindex, indices: Dict[str, pd.Series], basepath: _path_t):
        """Converter initialised with a set of IAMC variable index column defintions

        Parameters
        ----------
        idx : `friendly_data.dpkg.pkgindex`
            Index of datasets with IAMC variable definitions

        indices : Dict[str, pd.Series]
            Index column definitions

        """
        for col in indices:
            # fallback when title is missing
            _lvls = indices[col].fillna({i: i for i in indices[col].index})
            # for bidirectional lookup
            # indices[col] = _lvls.append(pd.Series({v:k for k, v in _lvls.items()}))
            indices[col] = _lvls
        self.indices = indices
        self.res_idx = pkgindex(glom(idx, Iter().filter(T["iamc"]).all()))
        self.basepath = Path(basepath)

    def _varwidx(self, entry: Dict, df: pd.DataFrame, basepath: _path_t) -> Resource:
        """Write a dataframe that includes index columns in the IAMC variable

        Parameters
        ----------
        entry : Dict
            Entry from the `friendly_data.dpkg.pkgindex`

        df : pd.DataFrame
            Data frame in IAMC format

        basepath : Union[str, Path]
            Data package base path

        Returns
        -------
        Resource
            The resource object pointing to the file that was written

        """
        _lvls = {
            col: self.indices[col]
            for col in entry["idxcols"]
            if col not in pyam.IAMC_IDX + ["year"]
        }
        # do a case-insensitive match
        values = {
            entry["iamc"]
            .format(**{k: v for k, v in zip(_lvls, vprod)})
            .lower(): "|".join(kprod)
            for kprod, vprod in zip(
                glom(_lvls.values(), Invoke(product).star([T.index])),
                glom(_lvls.values(), Invoke(product).star([T.values])),
            )
        }
        _df = df.reset_index("variable").query("variable.str.lower() == list(@values)")
        self._warn_empty(_df, entry)
        idxcols = _df.variable.str.lower().map(values).str.split("|", expand=True)
        idxcols.columns = _lvls.keys()
        # don't want to include _df["variable"] in results
        _df = (
            pd.concat([idxcols, _df["value"]], axis=1)
            .set_index(list(_lvls), append=True)
            .rename(columns={"value": self.f2col(entry["path"])})
        )
        return from_df(_df, basepath=basepath, datapath=entry["path"])

    def _varwnoidx(self, entry: Dict, df: pd.DataFrame, basepath: _path_t) -> Resource:
        """Write a dataframe that does not includes index columns in the IAMC variable

        Parameters
        ----------
        entry : Dict
            Entry from the `friendly_data.dpkg.pkgindex`

        df : pd.DataFrame
            Data frame in IAMC format

        basepath : Union[str, Path]
            Data package base path

        Returns
        -------
        Resource
            The resource object pointing to the file that was written

        """
        value = entry["iamc"].lower()
        _df = (
            df.reset_index("variable")
            .query("variable.str.lower() == @value")
            .drop(columns="variable")
            .rename(columns={"value": self.f2col(entry["path"])})
        )
        self._warn_empty(_df, entry)
        return from_df(_df, basepath=basepath, datapath=entry["path"])

    def from_iamdf(self, iamdf: pyam.IamDataFrame, basepath: _path_t) -> List[Resource]:
        """Write an IAMC dataframe

        Parameters
        ----------
        iamdf : pyam.IamDataFrame
            The IAMC data frame

        basepath : Union[str, Path]
            Data package base path

        Returns
        -------
        List[Resource]
            List of resource objects pointing to the files that were written

        """
        df = self.iamdf2df(iamdf)
        resources = [
            self._varwidx(entry, df, basepath)
            if is_fmtstr(entry["iamc"])
            else self._varwnoidx(entry, df, basepath)
            for entry in self.res_idx
        ]
        return resources

    def to_iamdf(self, files: Iterable[_path_t], output: _path_t = "") -> pd.DataFrame:
        dfs = []
        for fpath in files:
            _entries = [e for e in self.res_idx if f"{fpath}" == e["path"]]
            if _entries:
                entry = _entries[0]
                if len(_entries) > 1:
                    logger.warning(f"{entry['path']}: duplicate entries, picking first")
            else:
                continue
            df = to_df(res_from_entry(entry, self.basepath))
            lvls = {
                _lvl: self.indices[_lvl]
                for _lvl in df.index.names
                if _lvl not in pyam.IAMC_IDX + ["year"]
            }
            if is_fmtstr(entry["iamc"]):
                iamc_variable = (
                    df.index.set_levels(levels=lvls.values(), level=lvls.keys())
                    .to_frame()
                    .reset_index(list(lvls), drop=True)
                    .apply(lambda r: entry["iamc"].format(**r.to_dict()), axis=1)
                )
            else:
                iamc_variable = entry["iamc"]
            df = (
                df.rename(columns={df.columns[-1]: "value"})
                .reset_index(list(lvls), drop=True)
                .assign(variable=iamc_variable)
                .set_index("variable", append=True)
            )
            df.index = df.index.reorder_levels(pyam.IAMC_IDX + ["year"])
            dfs.append(df)
        df = pd.concat(dfs, axis=0)
        if output:
            Path(output).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output)
        return df
