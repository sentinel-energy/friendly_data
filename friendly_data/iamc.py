"""Interface to convert a Friendly dataset to IAMC format

Configuration can be done using two separate files, A global config file (in
YAML format) can set options like mapping an index column to the corresponding
IAMC names, and setting default values for mandatory columns.  Whereas per
dataset configuration like, identifying index columns, mapping a dataset to its
IAMC variable name, defining column aliases, and aggregations can be done in an
index file (in YAML format).

"""
from itertools import chain
from logging import getLogger
from pathlib import Path
from typing import cast, Dict, Iterable, List, Tuple, Union

from glom import glom, Iter, Match, MatchError, Or, T
import pandas as pd

from friendly_data._types import _path_t
from friendly_data.converters import _reader, resolve_aliases, to_df
from friendly_data.dpkg import pkgindex
from friendly_data.dpkg import res_from_entry
from friendly_data.helpers import idx_lvl_values, idxslice
from friendly_data.helpers import import_from
from friendly_data.helpers import filter_dict
from friendly_data.helpers import is_fmtstr
from friendly_data.io import dwim_file

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

    **TODO:**

    - describe assumptions (e.g. case insensitive match) and fallbacks
      (e.g. missing title)
    - limitations (e.g. when no index column exists)

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
            # prefer name over path because when name is present, it is more
            # likely to be more meaningful
            path_or_name = entry["name"] if "name" in entry else entry["path"]
            logger.warning(f"{path_or_name}: empty dataframe, check index entry")

    @classmethod
    def from_file(cls, confpath: _path_t, idxpath: _path_t) -> "IAMconv":
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
        return cls(pkgindex.from_file(idxpath), conf["indices"], basepath=basepath)

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

    @property
    def basepath(self):
        """Data package basepath, directory the index file is located"""
        return self._basepath

    @property
    def indices(self) -> Dict:
        """Index definitions

        - Default value of mandatory index columns in case they are missing

        - Different levels of user defined index columns; points to a 2-column
          CSV file, with the "name" and "iamc" columns

        """
        return self._indices

    @indices.setter
    def indices(self, indices: Dict):
        self._indices = {
            col: path_or_default
            if col in self._IAMC_IDX
            else self.read_indices(path_or_default, self.basepath)
            for col, path_or_default in indices.items()
        }

    @property
    def res_idx(self) -> pkgindex:
        """Package index

        Each entry corresponds to a resource that maybe included in IAMC output.

        """
        return self._res_idx

    @res_idx.setter
    def res_idx(self, idx: pkgindex):
        self._res_idx = pkgindex(glom(idx, Iter().filter(T.get("iamc")).all()))

    def __init__(self, idx: pkgindex, indices: Dict, basepath: _path_t):
        """Converter initialised with a set of IAMC variable index column defintions

        Parameters
        ----------
        idx : `friendly_data.dpkg.pkgindex`
            Index of datasets with IAMC variable definitions

        indices : Dict[str, Union[int, float, str, Path]]
            Index column definitions; a default value for an IAMC index column,
            path to a 2-column CSV file defining levels for a user defined
            index column (see :meth:`IAMconv.indices`)

        basepath : Union[str, Path]
            Top-level directory of the data package

        """
        self._basepath = Path(basepath)  # order important, needed by @indices.setter
        self.indices = indices
        self.res_idx = idx

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
        userdefined = set(idxcols) - set(self._IAMC_IDX)
        if len(userdefined) == 0:
            raise ValueError(f"idxcols={idxcols}: only for user defined idxcols")
        return filter_dict(self.indices, userdefined)

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

    def iamcify(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform dataframe to match the IAMC (long) format"""
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

    def _match_item(
        self, item: Union[_path_t, Tuple[str, pd.DataFrame]]
    ) -> Union[None, Tuple[Dict, pd.DataFrame]]:
        """Match a file or dataframe to an index entry (internal method)

        Parameters
        ----------
        item : Union[Union[str, Path], Tuple[str, pd.DataFrame]]
            The item to find in the index.  A file is matched with the ``path``
            key of the index entry, and the `key` of the dataframe is matched
            with the ``name`` key in the entry.

        Returns
        -------
        Tuple[Dict, pandas.DataFrame]
            The dictionary is the index entry; when ``item`` is a file, the
            index entry is used to read the file into a dataframe, and in case
            of a dataframe, it is passed on transparently.

        """
        if isinstance(item, tuple):
            match_key = "name"
            match_val = item[0]
        else:
            match_key = "path"
            match_val = f"{item}"

        # NOTE: res_from_entry requires: "path", "idxcols", "alias"; later
        # in the iteration, "iamc" & "agg" is required
        keys = [match_key, "idxcols", "alias", "iamc", "agg"]
        _entries = [
            entry
            for entry in self.res_idx.records(keys)
            # convert to string for path comparison
            if f"{match_val}" == entry[match_key]
        ]
        if _entries:
            entry = _entries[0]
            if len(_entries) > 1:
                logger.warning(f"{entry[match_key]}: duplicate entries, picking first")
        else:
            return None
        if isinstance(item, tuple):
            df = item[1]
        else:
            df = to_df(res_from_entry(entry, self.basepath))
        return entry, df

    def to_df(
        self, files_or_dfs: Union[Iterable[_path_t], Dict[str, pd.DataFrame]]
    ) -> pd.DataFrame:
        """Convert CSV files/dataframes to IAMC format according to the index

        Parameters
        ----------
        files_or_dfs : Union[Iterable[Union[str, Path]], Dict[str, pandas.DataFrame]]
            List of files or a dictionary of dataframes, to be collated and
            converted to IAMC format.  Each item must have an entry in the
            package index the converter was initialised with, it is skipped
            otherwise.  Files are matched by file ``path``, whereas dataframes
            match when the dictionary key matches the index entry ``name``.

            Note when the files are read, the basepath is set to whatever the
            converter was initialised with.  If :meth:`IAMconv.from_file` was
            used, it is the parent directory of the index file.

        Returns
        -------
        DataFrame
            A ``pandas.DataFrame`` in IAMC format

        """
        dfs = []
        if isinstance(files_or_dfs, dict):
            iterable = cast(Iterable, files_or_dfs.items())
        else:
            iterable = files_or_dfs

        for item in iterable:
            match = self._match_item(item)
            if match is None:
                continue
            res = self.frames(*match)  # match -> entry, dataframe
            dfs.append(res)
        df = pd.concat(chain.from_iterable(dfs), axis=0)
        if df.empty:
            logger.warning("empty data set, check config and index file")
        return df

    def frames(self, entry: Dict, df: pd.DataFrame) -> List[pd.DataFrame]:
        """Convert the dataframe to IAMC format according to configuration in the entry

        Parameters
        ----------
        entry : Dict
            Index entry

        df : pandas.DataFrame
            The dataframe that is to be converted to IAMC format

        Returns
        -------
        List[pandas.DataFrame]
            List of ``pandas.DataFrame``s in IAMC format

        """
        dfs = []
        df = resolve_aliases(df, entry.get("alias", {}))
        df = self.resolve_idxcol_defaults(df)
        lvls = self.index_levels(df.index.names)

        if entry["agg"]:  # None if not defined
            col, _agg_vals = self.agg_vals_all(entry)
            df_agg = cast(pd.DataFrame, df.query(f"{col} in @_agg_vals"))
            dfs.extend(self.agg_idxcol(df_agg, col, entry))

            _vals = lvls[col].index  # noqa: F841, used by query below
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
                col: vals.loc[vals.index.intersection(idx_lvl_values(df.index, col))]
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
        _df = self.iamcify(df.assign(variable=iamc_variable))
        dfs.append(_df)
        return dfs

    def to_csv(
        self,
        files: Iterable[_path_t],
        output: _path_t,
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
        df = self.to_df(files)
        if wide:
            df = pyam.IamDataFrame(df)
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output)
