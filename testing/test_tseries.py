from io import StringIO

import pandas._testing as tm
import pytest  # noqa: F401

from sark.tseries import read_timeseries, from_table, from_multicol


@pytest.mark.parametrize("zero_idx", [True, False])
@pytest.mark.parametrize(
    "tseries_table,units",
    [("MS", "month"), ("H", "hour")],
    indirect=["tseries_table"],
)
def test_from_table(tseries_table, units, zero_idx):
    if units == "month" and zero_idx is True:
        pytest.skip("months are not expected to be 0-indexed")

    df, expected = tseries_table

    if units == "hour" and zero_idx is False:
        df.columns = range(1, 25)
    CSV = StringIO(df.to_csv(None))
    result = from_table(CSV, col_units=units, zero_idx=zero_idx)
    tm.assert_series_equal(result, expected)


def test_from_multicol(tseries_multicol):
    df, expected = tseries_multicol
    CSV = StringIO(df.to_csv(None, index=False))
    result = from_multicol(CSV, date_cols=[0, 1])
    tm.assert_frame_equal(result, expected)

    # reordered columns
    df = df[["A", "B", "time", "date", "C", "D"]]
    CSV = StringIO(df.to_csv(None, index=False))
    result = from_multicol(CSV, date_cols=[3, 2])
    tm.assert_frame_equal(result, expected)


def test_read_timeseries(tseries_multicol, tseries_table):
    df, expected = tseries_table
    CSV = StringIO(df.to_csv(None))  # 0-indexed

    result = read_timeseries(
        CSV, source_t="table", col_units="hour", zero_idx=True
    )
    tm.assert_series_equal(result, expected)

    with pytest.raises(ValueError, match="col_units: .+"):
        read_timeseries(CSV, source_t="table")

    df, expected = tseries_multicol
    CSV = StringIO(df.to_csv(None, index=False))

    result = read_timeseries(CSV, source_t="multicol", date_cols=[0, 1])
    tm.assert_frame_equal(result, expected)

    with pytest.raises(ValueError, match="date_cols: .+"):
        read_timeseries(CSV, source_t="multicol")

    CSV.seek(0)  # reset stream to the beginning
    with pytest.warns(UserWarning, match="multi: .+"):
        result = read_timeseries(CSV, source_t="multi", date_cols=[0, 1])
        tm.assert_frame_equal(result, df.reset_index(drop=True))
