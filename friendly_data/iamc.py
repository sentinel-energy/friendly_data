from itertools import product
from logging import getLogger
from pathlib import Path
from typing import cast, Dict, Iterable, List, TYPE_CHECKING

from frictionless import Resource
from glom import glom, Iter, Invoke, Match, MatchError, Or, T
import pandas as pd

from friendly_data._types import _path_t
from friendly_data.converters import _reader, from_df, to_df
from friendly_data.dpkg import pkgindex
from friendly_data.dpkg import res_from_entry
from friendly_data.helpers import idx_lvl_values
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
        # only for user defined idxcols
        return {col: self.indices[col] for col in idxcols if col not in self._IAMC_IDX}

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
        _df = df.reset_index("variable").query("variable.str.lower() == list(@values)")
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

    def resolve_iamc_idxcol_defaults(self, df: pd.DataFrame):
        """Find missing IAMC indices and set them to the default value from config

        The IAMC format requires the following indices: `self._IAMC_IDX`; if
        any of them are missing, the corresponding index level is created, and
        the level values are set to a constant specified in the config.

        Parameters
        ----------
        df : pandas.DataFrame

        """
        defaults = filter_dict(self.indices, set(self._IAMC_IDX) - set(df.index.names))
        return df.assign(**defaults).set_index(list(defaults), append=True)

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
            # in the iteration, "iamc" is required
            _entries = [
                entry
                for entry in self.res_idx.records(["path", "idxcols", "alias", "iamc"])
                if f"{fpath}" == entry["path"]
            ]
            if _entries:
                entry = _entries[0]
                if len(_entries) > 1:
                    logger.warning(f"{entry['path']}: duplicate entries, picking first")
            else:
                continue
            df = to_df(res_from_entry(entry, basepath if basepath else self.basepath))
            df = self.resolve_iamc_idxcol_defaults(df)
            lvls = self.index_levels(df.index.names)
            if is_fmtstr(entry["iamc"]):
                # NOTE: need to calculate the subset of levels that are in the
                # current dataframe; this is b/c MultiIndex.set_levels accepts
                # a sequence of level values for every level FIXME: check if
                # data in file is consistent with index definition
                _lvls = {col: lvls[col][idx_lvl_values(df.index, col)] for col in lvls}
                iamc_variable = (
                    df.index.set_levels(levels=_lvls.values(), level=_lvls.keys())
                    .to_frame()
                    .reset_index(list(_lvls), drop=True)
                    .apply(lambda r: entry["iamc"].format(**r.to_dict()), axis=1)
                )
            else:
                iamc_variable = entry["iamc"]
            useridxlvls = list(set(df.index.names) - set(self._IAMC_IDX))
            # ensure all user defined index columns are removed before concatinating
            df = (
                df.rename(columns={df.columns[-1]: "value"})
                .reset_index(useridxlvls, drop=True)
                .assign(variable=iamc_variable)
                .set_index("variable", append=True)
            )
            df.index = df.index.reorder_levels(self._IAMC_IDX)
            dfs.append(df)
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
