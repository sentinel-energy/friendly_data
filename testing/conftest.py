from pathlib import Path
from shutil import copytree

import pandas as pd
import pandas._testing as tm
import pytest

from sark.dpkg import fullpath
from sark.dpkg import read_pkg
from sark.io import HttpCache
from sark.metatools import ODLS


class noop_map(dict):
    def __missing__(self, key):
        return key


default_type_map = {
    "object": "string",
    "float64": "number",
    "int64": "integer",
    "Int64": "integer",
    "bool": "boolean",
}


def expected_schema(df, type_map=default_type_map):
    type_map = noop_map(type_map)
    # handle a resource and a path
    if not isinstance(df, pd.DataFrame):
        try:
            df = pd.read_csv(fullpath(df))
        except AttributeError:
            df = pd.read_csv(df)  # path
    # noop if a key (type) is not in `type_map`, remains unaltered
    return df.dtypes.astype(str).map(type_map).to_dict()


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
def tmp_pkgdir(tmp_path):
    src = Path("testing/files/mini-ex")
    dest = tmp_path / "pkg"
    copytree(src, dest)
    return src, dest


@pytest.fixture
def pkg():
    pkgdir = Path("testing/files/mini-ex")
    dpkg_json = pkgdir / "datapackage.json"
    return read_pkg(dpkg_json)


@pytest.fixture
def rnd_pkg():
    pkgdir = Path("testing/files/random")
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
