from pathlib import Path

import pytest

from sark.dpkg import create_pkg
from sark.io import HttpCache
from sark.metatools import get_license, ODLS


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
def datadir():
    return Path("testing/data")


@pytest.fixture
def pkg(datadir):
    pkg_meta = {
        "name": "test",
        "title": "test",
        "licenses": get_license("CC0-1.0"),
    }
    return create_pkg(pkg_meta, datadir.glob("*.csv"))
