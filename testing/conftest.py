import io
from pathlib import Path

import pandas as pd
import pandas._testing as tm
import pytest

from sark.dpkg import read_pkg
from sark.io import HttpCache
from sark.metatools import ODLS


# values as per noop
default_type_map = {
    "object": "string",
    "float64": "number",
    "int64": "integer",
    "Int64": "integer",
    "bool": "boolean",
}


def expected_schema(df, type_map=default_type_map):
    # handle a resource and a path
    if not isinstance(df, pd.DataFrame):
        try:
            df = pd.read_csv(df.source)
        except AttributeError:
            df = pd.read_csv(df)  # path
    # datapackage.Package.infer(..) relies on tableschema.Schema.infer(..),
    # which infers datetime as string
    return (
        df.dtypes.astype(str)
        .map(lambda t: type_map[t] if t in type_map else t)
        .to_dict()
    )


@pytest.fixture
def http_cache(request):
    # request: special object to parametrize fixtures
    http_cache = HttpCache(request.param)
    yield http_cache
    try:  # remove cache before next test
        http_cache.remove()
    except FileNotFoundError:
        # some tests do not create a cache; the safeguard is probably not
        # really required because HttpCache.remove() uses glob, which returns
        # an empty iterator when there are no files.
        pass


@pytest.fixture
def clean_odls_cache():
    # hack to cleanup cache files
    yield
    http_cache = HttpCache(ODLS)
    http_cache.remove()


@pytest.fixture
def pkgdir():
    return Path("testing/files/random")


@pytest.fixture
def pkg(pkgdir):
    dpkg_json = pkgdir / "datapackage.json"
    return read_pkg(dpkg_json)


@pytest.fixture
def tseries_table(request):
    freq = getattr(request, "param", "H")  # hourly by default
    assert freq in ("H", "MS")

    ts = tm.makeTimeSeries(nper=100, freq=freq)
    # unset the freq attribute, makes test fail, not relevant for us
    ts.index.freq = None
    ts_tbl = ts.copy(deep=True)
    if freq == "H":
        date_cols = [pd.to_datetime(ts.index.date), ts.index.hour]
    elif freq == "MS":  # month start
        date_cols = [ts.index.year, ts.index.month]
    ts_tbl.index = pd.MultiIndex.from_arrays(date_cols)
    return ts_tbl.unstack(), ts


@pytest.fixture
def tseries_multicol():
    fmt = "%Y-%m-%d %H:%M:%S"
    ts = pd.DataFrame(tm.getTimeSeriesData(nper=100, freq="H"))
    # unset the freq attribute, makes test fail, not relevant for us
    ts.index.freq = None
    ts_multicol = pd.concat(
        [ts.index.to_series().dt.strftime(fmt).str.split(expand=True), ts],
        axis=1,
    )
    ts_multicol.columns = ["date", "time", "A", "B", "C", "D"]
    ts.index.name = "date_time"  # concatenated column names
    return ts_multicol, ts
