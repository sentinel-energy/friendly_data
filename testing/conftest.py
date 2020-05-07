from pathlib import Path

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
