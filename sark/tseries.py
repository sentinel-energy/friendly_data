from pathlib import Path
from typing import List, TypeVar, Union
from warnings import warn

import pandas as pd

_time_units = (
    "days",
    "day",
    "hours",
    "hour",
    "minutes",
    "minute",
    "seconds",
    "second",
)

_path_t = Union[str, Path]
_col_t = TypeVar("_col_t", int, str)


def read_timeseries(
    fpath: _path_t,
    *,
    date_cols: Union[List[_col_t], None] = None,
    col_units: Union[str, None] = None,
    zero_idx: bool = False,
    source_t: str = "",
    **kwargs,
):
    """Read a time series from a file.

    While the natural way to structure a time series dataset is with the index
    column as datetime values, with subsequent columns holding other values,
    there are a few other frequently used structures.

    The first is to structure it as a table:

    ===========  ===  ===  ====  ====  ====
                  1    2    ..    23    24
    ===========  ===  ===  ====  ====  ====
     1/1/2016      0   10    ..   2.3   5.1
     4/1/2016      3   11    ..   4.3   9.1
    ===========  ===  ===  ====  ====  ====

    When `source_t` is set to "table", this function reads a tabular dataset,
    like the one above, and flattens it into a series, while setting the
    appropriate datetime values as their index.

    The other common structure is to split the datetime values into multiple
    columns in the table:

    ===========  ======  ======  ======
      date        time    col1    col2
    ===========  ======  ======  ======
     1/1/2016     10:00    42.0    foo
     4/1/2016     11:00    3.14    bar
    ===========  ======  ======  ======

    When `source_t` is set to "multicol". this function the table, while
    combining the designated columns to construct the datetime values, which
    are then set as the index.

    If `source_t` is not specified (or set to an empty string), options
    specific to this function are ignored, and all other keyword options are
    passed on to the backend transparently; in case of reading a CSV with
    Pandas, that would include all valid keywords for `pandas.read_csv`.

    Parameters
    ----------
    fpath : Union[str, Path]
        Path to the dataset file

    date_cols : List[int, str] (for "multicol" mode)
        List of columns to be combined to construct the datetime values

    col_units : str (for "table" mode)
        Time units for the columns.  Accepted values are: "days", "day",
        "hours", "hour", "minutes", "minute", "seconds", "second".

    zero_idx : bool (for "table" mode)
        Whether the columns are zero indexed.  When the columns represent
        hours, or minutes, it is common to number them as nth hour.  Which
        means they are counted starting at 1 instead of 0.  Set this to False
        if that is the case.

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
            fpath, col_units=col_units, zero_idx=zero_idx, **kwargs,
        )
    elif source_t == "multicol":
        if date_cols is None:
            raise ValueError("date_cols: missing list of datetime columns")
        ts = from_multicol(fpath, date_cols=date_cols, **kwargs,)
    else:
        if source_t:
            warn(f"{source_t}: unsupported source, falling back to default")
        ts = pd.read_csv(fpath, **kwargs)
    return ts


def from_table(fpath: _path_t, *, col_units: str, zero_idx: bool, **kwargs):
    """Read a time series from a tabular file.

    See Also
    --------
    read_timeseries : see for full documentation, main entrypoint for users

    """
    assert col_units in _time_units

    # NOTE: assumption: input is oriented as portrait
    ts = pd.read_csv(
        fpath, **{"parse_dates": True, "index_col": 0, **kwargs}
    ).stack()
    # FIXME: do we also need support for custom stack level?  How common is a
    # hierarchical index for columns

    # merge indices
    ts_idx = pd.DataFrame(ts.index.to_list())
    ts_delta = pd.to_timedelta(
        ts_idx.iloc[:, 1].astype(int) - int(not zero_idx), unit=col_units
    )
    ts.index = ts_idx.iloc[:, 0] + ts_delta
    return ts


def from_multicol(fpath: _path_t, *, date_cols: List[_col_t], **kwargs):
    """Read a time series where datetime values are in multiple columns.

    See Also
    --------
    read_timeseries : see for full documentation, main entrypoint for users

    """
    df = pd.read_csv(
        fpath, parse_dates=[date_cols], index_col=date_cols[0], **kwargs
    )
    return df
