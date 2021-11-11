"""Interface to convert a Friendly dataset to IAMC format

Configuration can be done using two separate files, A global config file (in
YAML format) can set options like mapping an index column to the corresponding
IAMC names, and setting default values for mandatory columns.  Whereas per
dataset configuration like, identifying index columns, mapping a dataset to its
IAMC variable name, defining column aliases, and aggregations can be done in an
index file (in YAML format).

"""
from itertools import product
from logging import getLogger
from pathlib import Path
from typing import cast, Dict, Iterable, List, TYPE_CHECKING, Tuple

from frictionless import Resource
from glom import glom, Iter, Invoke, Match, MatchError, Or, T
import pandas as pd

from friendly_data._types import _path_t
from friendly_data.converters import _reader, from_df, to_df
from friendly_data.dpkg import pkgindex
from friendly_data.dpkg import res_from_entry
from friendly_data.helpers import idx_lvl_values, idxslice
from friendly_data.helpers import import_from
from friendly_data.helpers import filter_dict
from friendly_data.helpers import is_fmtstr
from friendly_data.io import dwim_file

if TYPE_CHECKING:
    from pyam import IamDataFrame

# weak dependency on pyam; damn plotly!
pyam = import_from("pyam", "")

logger = getLogger(__name__)


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

    FIXME:
    - basepath insconsistency
    - df/iamdf/csv inconsistency

    """

    _IAMC_IDX = pyam.IAMC_IDX + ["year"]

    @classmethod
    def _validate(cls, conf: Dict) -> Dict:
        # FIXME: check if file exists for user defined idxcols
        conf_match = Match(
            {
                "indices": {str: Or(str, int)},  # int for year
                str: object,  # fall through for other config keys
            }
        )
        try:
            return glom(conf, conf_match)
        except MatchError as err:
            logger.exception(
                f"{err.args[1]}: must define a dictionary of files pointing to idxcol"
                "definitions for IAMC conversion, or set a default value for one of:"
                f"{', '.join(cls._IAMC_IDX)}"
            )
            raise err from None

    @classmethod
    def _warn_empty(cls, df: pd.DataFrame, entry: Dict):
        if df.empty:
            logger.warning(f"{entry['path']}: empty dataframe, check index entry")

    @classmethod
    def iamdf2df(cls, iamdf: "IamDataFrame") -> pd.DataFrame:
        """Convert :class:`pyam.IamDataFrame` to :class:`pandas.DataFrame`"""
        return iamdf.as_pandas().drop(columns="exclude").set_index(cls._IAMC_IDX)

    @classmethod
    def f2col(cls, fpath: _path_t) -> str:
        """Deduce column name from file name"""
        return Path(fpath).stem

    @classmethod
    def from_iamdf_simple(
        cls, iamdf: "IamDataFrame", basepath: _path_t, datapath: _path_t
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
        basepath = Path(idxpath).parent
        conf = cls._validate(cast(Dict, dwim_file(confpath)))
        return cls(
            pkgindex.from_file(idxpath), conf["indices"], basepath=basepath, **kwargs
        )

    @classmethod
    def read_indices(cls, path: _path_t, basepath: _path_t, **kwargs) -> pd.Series:
        """Read index column definitions provided in config"""
        _lvls: pd.Series = _reader(
            Path(basepath) / path,
            usecols=["name", "iamc"],
            index_col="name",
            squeeze=True,
            **kwargs,
        )
        # fallback when iamc name is missing; capitalized name is the most common
        return _lvls.fillna({i: i.capitalize() for i in _lvls.index})

    def __init__(self, idx: pkgindex, indices: Dict, basepath: _path_t, **kwargs):
        """Converter initialised with a set of IAMC variable index column defintions

        Parameters
        ----------
        idx : `friendly_data.dpkg.pkgindex`
            Index of datasets with IAMC variable definitions

        indices : Dict[str, pd.Series]
            Index column definitions

        basepath : Union[str, Path]
            Path where the IAMC output will be written

        """
        # levels are for user defined idxcols, default for mandatory idxcols
        self.indices = {
            col: path_or_default
            if col in self._IAMC_IDX
            else self.read_indices(path_or_default, basepath, **kwargs)
            for col, path_or_default in indices.items()
        }
        self.res_idx = pkgindex(glom(idx, Iter().filter(T.get("iamc")).all()))
        self.basepath = Path(basepath)

    def index_levels(self, idxcols: Iterable) -> Dict[str, pd.Series]:
        """Index levels for user defined index columns

        Parameters
        ----------
        idxcols : Iterable[str]
            Iterable of index column names

        Returns
        -------
        Dict[str, pd.Series]
            Different values for a given set of index columns

        """
        # only for user defined idxcols
        return filter_dict(self.indices, set(idxcols) - set(self._IAMC_IDX))

    def resolve_idxcol_defaults(self, df: pd.DataFrame) -> pd.DataFrame:
        """Find missing IAMC indices and set them to the default value from config

        The IAMC format requires the following indices: `self._IAMC_IDX`; if
        any of them are missing, the corresponding index level is created, and
        the level values are set to a constant specified in the config.

        Parameters
        ----------
        df : pandas.DataFrame

        Returns
        -------
        pandas.DataFrame
            Dataframe with default index columns resolved

        """
        defaults = filter_dict(self.indices, set(self._IAMC_IDX) - set(df.index.names))
        return cast(
            pd.DataFrame, df.assign(**defaults).set_index(list(defaults), append=True)
        )

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
        _lvls = self.index_levels(entry["idxcols"])
        # do a case-insensitive match
        values = {
            entry["iamc"].format(**dict(zip(_lvls, vprod))).lower(): "|".join(kprod)
            for kprod, vprod in zip(
                glom(_lvls.values(), Invoke(product).star([T.index])),
                glom(_lvls.values(), Invoke(product).star([T.values])),
            )
        }
        _df = cast(
            pd.DataFrame,
            df.reset_index("variable").query("variable.str.lower() == list(@values)"),
        )
        self._warn_empty(_df, entry)
        # FIXME: maybe instead of str.split, put a tuple, and expand
        idxcols = _df.variable.str.lower().map(values).str.split("|", expand=True)
        idxcols.columns = _lvls.keys()
        # don't want to include _df["variable"] in results
        _df = (
            pd.concat([idxcols, _df["value"]], axis=1)
            .set_index(list(_lvls), append=True)
            .rename(columns={"value": self.f2col(entry["path"])})
        )  # FIXME: maybe add a column spec in index entries
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

    def from_iamdf(self, iamdf: "IamDataFrame", basepath: _path_t) -> List[Resource]:
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

    def iamcify(self, df: pd.DataFrame) -> pd.DataFrame:
        useridxlvls = list(set(df.index.names) - set(self._IAMC_IDX))
        # ensure all user defined index columns are removed before concatinating
        df = (
            df.rename(columns={df.columns[0]: "value"})
            .set_index("variable", append=True)
            .reset_index(useridxlvls, drop=True)
        )
        df.index = df.index.reorder_levels(self._IAMC_IDX)
        return df

    def agg_idxcol(self, df: pd.DataFrame, col: str, entry: Dict) -> List[pd.DataFrame]:
        """Aggregate values and generate IAMC dataframes

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe to aggregate from

        col : str
            Column to perform aggregation on

        entry : Dict
            Index entry with aggregation rules

        Returns
        -------
        List[pd.DataFrame]
            List of IAMC dataframes

        """
        dfs = []
        for lvls, var in glom(entry["agg"][col], [(T.values(), tuple)]):
            rest = df.index.names.difference([col])
            _df = cast(
                pd.DataFrame,
                df.query(f"{col} in @lvls").groupby(rest).sum().assign(variable=var),
            )
            dfs.append(self.iamcify(_df))
        return dfs

    def agg_vals_all(self, entry: Dict) -> Tuple[str, List[str]]:
        """Find all values in index column that are present in an aggregate rule"""
        assert len(entry["agg"]) == 1, "only support aggregating one column"
        col, conf = entry["agg"].copy().popitem()
        vals = glom(conf, (Iter().map(T["values"]).flatten().all(), set, list))
        return col, vals

    def to_df(self, files: Iterable[_path_t], basepath: _path_t = "") -> pd.DataFrame:
        """Convert CSV files to IAMC format according to configuration in the index

        Parameters
        ----------
        files : Iterable[Union[str, Path]]
            List of files to collate and convert to IAMC

        basepath : Union[str, Path]
            Data package base path

        Returns
        -------
        DataFrame
            A ``pandas.DataFrame`` in IAMC format

        """
        dfs = []
        for fpath in files:
            # NOTE: res_from_entry requires: "path", "idxcols", "alias"; later
            # in the iteration, "iamc" & "agg" is required
            keys = ["path", "idxcols", "alias", "iamc", "agg"]
            _entries = [
                entry
                for entry in self.res_idx.records(keys)
                if f"{fpath}" == entry["path"]
            ]
            if _entries:
                entry = _entries[0]
                if len(_entries) > 1:
                    logger.warning(f"{entry['path']}: duplicate entries, picking first")
            else:
                continue
            df = to_df(res_from_entry(entry, basepath if basepath else self.basepath))
            df = self.resolve_idxcol_defaults(df)
            lvls = self.index_levels(df.index.names)

            if entry["agg"]:
                col, _agg_vals = self.agg_vals_all(entry)
                df_agg = cast(pd.DataFrame, df.query(f"{col} in @_agg_vals"))
                dfs.extend(self.agg_idxcol(df_agg, col, entry))

                _vals = lvls[col].index
                df = cast(pd.DataFrame, df.query(f"{col} in @_vals"))

                # NOTE: need to remove aggregated levels, then calculate the
                # intersection with the levels that are in the current dataframe
                _lvls = {
                    col: vals.loc[
                        vals.index.difference(_agg_vals).intersection(
                            idx_lvl_values(df.index, col)
                        )
                    ]
                    for col, vals in lvls.items()
                }
            else:
                # NOTE: need to calculate the intersection of levels that are
                # in the current dataframe and the levels defined in the config
                _lvls = {
                    col: vals.loc[
                        vals.index.intersection(idx_lvl_values(df.index, col))
                    ]
                    for col, vals in lvls.items()
                }

            if is_fmtstr(entry["iamc"]):
                sel = idxslice(
                    df.index.names,
                    {col: val.index for col, val in _lvls.items()},
                )
                df = df.loc[sel]
                df.index = df.index.remove_unused_levels()
                iamc_variable = pd.DataFrame(
                    {
                        col: df.index.get_level_values(col).map(val)
                        for col, val in _lvls.items()
                    },
                    index=df.index,
                ).apply(lambda r: entry["iamc"].format(**r.to_dict()), axis=1)
            else:
                iamc_variable = entry["iamc"]
            df = df.assign(variable=iamc_variable)
            dfs.append(self.iamcify(df))
        df = pd.concat(dfs, axis=0)
        if df.empty:
            logger.warning("empty data set, check config and index file")
        return df

    def to_csv(
        self,
        files: Iterable[_path_t],
        output: _path_t,
        basepath: _path_t = "",
        wide: bool = False,
    ):
        """Write converted IAMC data frame to a CSV file

        Parameters
        ----------
        files : Iterable[Union[str, Path]]
            List of files to collate and convert to IAMC

        output : Union[str, Path] (default: empty string)
            Path of the output CSV file; if empty, nothing is written to file.

        basepath : Union[str, Path]
            Data package base path

        wide : bool (default: False)
            Write the CSN in wide format (with years as columns)

        """
        df = self.to_df(files, basepath=basepath)
        if wide:
            df = pyam.IamDataFrame(df)
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output)
