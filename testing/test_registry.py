from itertools import chain
import logging
from pathlib import Path

from pkg_resources import resource_filename

import pytest

import friendly_data_registry as _registry


@pytest.mark.parametrize("col, col_t", [("region", "idxcols"), ("storage", "cols")])
def test_registry(col, col_t):
    res = _registry.get(col, col_t)
    assert isinstance(res, dict)


@pytest.mark.parametrize(
    "col, col_t, msg", [("notinreg", "cols", "notinreg: not in registry")]
)
def test_registry_warn(caplog, col, col_t, msg):
    with caplog.at_level(logging.INFO, logger="friendly_data._registry"):
        res = _registry.get(col, col_t)
        assert isinstance(res, dict)
        assert res == {}
        assert msg in caplog.text


@pytest.mark.parametrize(
    "col, col_t, expectation",
    [
        (
            "timesteps",
            "bad_col_t",
            pytest.raises(ValueError, match="bad_col_t: unknown column type"),
        ),
    ],
)
def test_registry_raise(col, col_t, expectation):
    with expectation:
        res = _registry.get(col, col_t)
        assert isinstance(res, dict)


def test_getall():
    res = _registry.getall()
    expected = ["cols", "idxcols"]
    assert sorted(res) == expected
    for col_t in expected:
        curdir = Path(resource_filename("friendly_data_registry", col_t))
        schemas = list(
            chain.from_iterable(curdir.glob(f"*.{fmt}") for fmt in ("json", "yaml"))
        )
        assert len(res[col_t]) == len(schemas)
