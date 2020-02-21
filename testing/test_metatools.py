import json

import pytest

from sark.metatools import ODLS, get_license, check_license
from sark.io import HttpCache


def test_license_get():
    lic = "CC-BY-SA-4.0"
    assert lic == get_license(lic, group="all")["name"]
    with pytest.raises(KeyError):
        get_license(lic, group="osi")

    with pytest.raises(ValueError):
        get_license(lic, group="foo")

    # hack to cleanup cache files
    http_cache = HttpCache(ODLS)
    http_cache.remove()


@pytest.mark.parametrize(
    "http_cache, lic_id, log_warn",
    [
        (ODLS, "CC0-1.0", None),
        (ODLS, "Apache-1.1", "active"),
        (ODLS, "Apache-2.0", "data"),
    ],
    indirect=["http_cache"],
)
def test_license_check(caplog, http_cache, lic_id, log_warn):
    licenses = json.loads(http_cache.get("all"))
    assert check_license(licenses[lic_id])

    if log_warn:
        assert "inappropriate license" in caplog.text
        assert log_warn in caplog.text
        for record in caplog.records:
            assert record.levelname == "WARNING"
