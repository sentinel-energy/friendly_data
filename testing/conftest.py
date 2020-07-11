from pathlib import Path

import pandas as pd
import pandas._testing as tm
import pytest

from sark.dpkg import read_pkg
from sark.io import HttpCache
from sark.metatools import ODLS


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
    return Path("testing/files")


@pytest.fixture
def pkg(pkgdir):
    dpkg_json = pkgdir / "datapackage.json"
    return read_pkg(dpkg_json)


@pytest.fixture
def tseries_table():
    ts = tm.makeTimeSeries(nper=100, freq="h")
    ts_tbl = ts.copy(deep=True)
    ts_tbl.index = pd.MultiIndex.from_arrays(
        [pd.to_datetime(ts.index.date), ts.index.hour]
    )
    return ts_tbl.unstack(), ts


@pytest.fixture
def tseries_multicol():
    ts = pd.DataFrame(tm.getTimeSeriesData(nper=100, freq="h"))
    ts_multicol = pd.concat(
        [
            ts.index.to_series()
            .dt.strftime("%Y-%m-%d %H:%M:%S")
            .str.split(expand=True),
            ts,
        ],
        axis=1,
    )
    ts_multicol.columns = ["date", "time", "A", "B", "C", "D"]
    ts.index.name = "date_time"  # concatenated column names
    return ts_multicol, ts
