import pytest
import requests

from sark.io import HttpCache
from sark.metatools import ODLS

BAD_URL = ODLS[:-1]
BAD_CONN = "https://iamnota.site/{}"


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


params_all = [
    ODLS,
    pytest.param(
        BAD_URL,
        marks=pytest.mark.xfail(raises=ValueError, reason="Incorrect url"),
    ),
    pytest.param(
        BAD_CONN,
        marks=pytest.mark.xfail(
            raises=requests.ConnectionError,
            reason="Non-existent domain, similar to no network",
        ),
    ),
]


@pytest.mark.parametrize(
    "http_cache", [ODLS], indirect=["http_cache"],
)
def test_http_cache_file(http_cache):
    assert http_cache.cachedir.exists()
    assert http_cache.cachefile("foo")[0] == http_cache.cachefile("foo")[0]
    assert http_cache.cachefile("foo")[0] != http_cache.cachefile("bar")[0]


@pytest.mark.parametrize(
    "http_cache", params_all, indirect=["http_cache"],
)
def test_http_cache_fetch(http_cache):
    grp = "all"
    contents = http_cache.fetch(http_cache.cachefile(grp)[1])
    assert contents
    assert isinstance(contents, bytes)
    assert not http_cache.cachefile(grp)[0].exists()  # cache not created


@pytest.mark.parametrize(
    "http_cache", params_all, indirect=["http_cache"],
)
def test_http_cache_get(http_cache):
    grp = "all"
    contents = http_cache.get(grp)
    assert contents
    assert isinstance(contents, bytes)
    assert http_cache.cachefile(grp)[0].exists()  # cache created


@pytest.mark.parametrize(
    "http_cache", [ODLS], indirect=["http_cache"],
)
def test_http_cache_many_gets(http_cache):
    grps = ["all", "osi"]
    contents, caches = [], []
    for grp in grps:
        contents += [http_cache.get(grp)]
        caches += [http_cache.cachefile(grp)[0]]

    assert contents[0] != contents[1]
    assert caches[0] != caches[1]
    assert caches[0].read_bytes() != caches[1].read_bytes()
