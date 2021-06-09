"""Convenience functions useful to ingest different kinds of differently shaped
time series data into the standard 1-D shape supported by the data package
specification.

"""

from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import List, TextIO, TypeVar, Union
import warnings

import pandas as pd

logger = getLogger(__name__)

_file_t = TypeVar("_file_t", str, Path, TextIO)
_col_t = TypeVar("_col_t", int, str)


def read_timeseries(
    fpath: _file_t,
    *,
    date_cols: Union[List[_col_t], None] = None,
    col_units: Union[str, None] = None,
    zero_idx: bool = False,
    row_fmt: str = "",
    source_t: str = "",
    **kwargs,
):
    """Read a time series from a file.

    While the natural way to structure a time series dataset is with the index
    column as datetime values, with subsequent columns holding other values,
    there are a few other frequently used structures.

    The first is to structure it as a table:

    ===========  ===  ===  =====  ====  ====
     date         1    2    ...    23    24
    ===========  ===  ===  =====  ====  ====
     1/1/2016      0   10   ...    2.3   5.1
     4/1/2016      3   11   ...    4.3   9.1
    ===========  ===  ===  =====  ====  ====

    When `source_t` is set to "table", this function reads a tabular dataset
    like the one above, and flattens it into a series, and sets the appropriate
    datetime values as their index.

    The other common structure is to split the datetime values into multiple
    columns in the table:

    ===========  ======  ======  ======
      date        time    col1    col2
    ===========  ======  ======  ======
     1/1/2016     10:00    42.0    foo
     4/1/2016     11:00    3.14    bar
    ===========  ======  ======  ======

    When `source_t` is set to "multicol", as the table is read, the indicated
    columns are combined to construct the datetime values, which are then set
    as the index.

    If `source_t` is not specified (or set to an empty string), options
    specific to this function are ignored, and all other keyword options are
    passed on to the backend transparently; in case of reading a CSV with
    Pandas, that means all valid keywords for `pandas.read_csv` are accepted.

    Parameters
    ----------
    fpath : Union[str, Path, TextIO]
        Path to the dataset file

    date_cols : List[int, str] (for "multicol" mode)
        List of columns to be combined to construct the datetime values

    col_units : str (for "table" mode)
        Time units for the columns.  Accepted values: "month", "hour".

    zero_idx : bool (for "table" mode, default: False)
        Whether the columns are zero indexed.  When the columns represent
        hours, or minutes, it is common to number them as nth hour.  Which
        means they are counted starting at 1 instead of 0.  Set this to False
        if that is the case.

    row_fmt : str (for "table" mode, default: empty string)
        What is the format of the datetime column (use strftime format strings,
        see: `man 3 strftime`).  If this is left empty, the reader tries to
        guess a format using the `dateutil` module (Pandas default)

    source_t : str (default: empty string)
        Mode of reading the data. Accepted values: "table", "multicol", or
        empty string

    **kwards : Dict
        Other keyword arguments passed on to the reader backend.  Any options
        passed here takes precedence, and overwrites other values inferred from
        the earlier keyword arguments.

    Returns
    -------
    ts : Series/DataFrame
        The time series is returned as a series or a dataframe depending on the
        number of other columns that are present.

    Examples
    --------

    To skip specific rows, maybe because they have bad data, or are empty, you
    may use the `skiprows` option.  It can be set to a list-like where the
    entries are row indices (numbers).

    >>> read_timeseries("mydata.csv", source_t="table", col_units="hour",
    ...     skiprows=range(1522, 5480))  # doctest: +SKIP

    The above example skips rows 1522-5480.

    Similarly, data type of the column values can be controlled by using the
    `dtype` option.  When set to a `numpy.dtype`, all values will be read as
    that type, which is probably relevant for the "table" mode.  In the
    "multicol" mode, the types of the values can be controlled at the column
    level by setting it to a dictionary, where the key matches a column name,
    and the value is a valid `numpy.dtype`.

    """
    # FIXME: parse_dates & index_col assumes input is oriented as portrait
    if source_t == "table":
        if col_units is None:
            raise ValueError("col_units: missing time unit for columns")
        ts = from_table(
            fpath,
            col_units=col_units,
            zero_idx=zero_idx,
            row_fmt=row_fmt,
            **kwargs,
        )
    elif source_t == "multicol":
        if date_cols is None:
            raise ValueError("date_cols: missing list of datetime columns")
        ts = from_multicol(fpath, date_cols=date_cols, **kwargs)
    else:
        if source_t:
            logger.warning(f"{source_t}: unsupported source, falling back to default")
        ts = pd.read_csv(fpath, **kwargs)
    return ts


def from_table(
    fpath: _file_t,
    *,
    col_units: str,
    zero_idx: bool,
    row_fmt: str = "",
    **kwargs,
):
    """Read a time series from a tabular file.

    See Also
    --------
    read_timeseries : see for full documentation, main entrypoint for users

    """
    # NOTE: allow for plural forms, as it is quite common, but the allowance is
    # undocumented, hence not guaranteed to work.
    if "month" in col_units:
        offset = pd.tseries.offsets.MonthBegin()
    elif "hour" in col_units:
        offset = pd.Timedelta(1, unit="hour")
    else:
        raise ValueError(f"{col_units}: unsupported column units")

    # NOTE: assumption: input is oriented as portrait
    opts = {"parse_dates": [0], "index_col": 0}
    # NOTE: for date-hour, it's okay to use the default dateutil parser for
    # date, unless otherwise specified, however for year-month it gets confused
    # and the format string needs to be explicitly set to YYYY
    if col_units == "month" and row_fmt == "":
        row_fmt = "%Y"
    if row_fmt:
        opts.update(date_parser=lambda dt: datetime.strptime(dt, row_fmt))
    # NOTE: "parse_dates", and "index_col" maybe overidden by the keyword
    # arguments so that the user has the option to ignore the inferred values;
    # it's a wild world, can't think of everything ;)
    opts.update(kwargs)
    ts = pd.read_csv(fpath, **opts).stack()

    # merge indices
    idx_lvls = [ts.index.get_level_values(i) for i in (0, 1)]
    ts_delta = (idx_lvls[1].astype(int) - int(not zero_idx)) * offset
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=pd.errors.PerformanceWarning)
        ts.index = idx_lvls[0] + ts_delta
    return ts


def from_multicol(fpath: _file_t, *, date_cols: List[_col_t], **kwargs):
    """Read a time series where datetime values are in multiple columns.

    See Also
    --------
    read_timeseries : see for full documentation, main entrypoint for users

    """
    # NOTE: index_col=0 b/c columns parsed as dates always end up in the front
    df = pd.read_csv(fpath, parse_dates=[date_cols], index_col=0, **kwargs)
    return df
