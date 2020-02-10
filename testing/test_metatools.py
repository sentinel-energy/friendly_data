import pytest

from sark import get_license
from sark.metatools import ODLS
from sark.io import HttpCache


@pytest.fixture
def cleanup_cache():  # hack to cleanup cache files
    yield
    http_cache = HttpCache(ODLS)
    http_cache.remove()


def test_license(cleanup_cache):
    lic = "CC-BY-SA-4.0"
    assert lic == get_license(lic, group="all")["id"]
    with pytest.raises(KeyError):
        assert lic == get_license(lic, group="osi")["id"]
