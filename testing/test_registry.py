from contextlib import nullcontext as does_not_raise
from itertools import chain
from pathlib import Path

from pkg_resources import resource_filename

import pytest

import friendly_data_registry as registry


@pytest.mark.parametrize(
    "col, col_t, expectation",
    [
        ("locs", "idxcols", does_not_raise()),
        ("storage", "cols", does_not_raise()),
        (
            "notinreg",
            "cols",
            pytest.warns(RuntimeWarning, match="notinreg: not in registry"),
        ),
        (
            "timesteps",
            "bad_col_t",
            pytest.raises(ValueError, match="bad_col_t: unknown column type"),
        ),
    ],
)
def test_registry(col, col_t, expectation):
    with expectation:
        res = registry.get(col, col_t)
        assert isinstance(res, dict)
        if col == "notinreg":
            assert res == {}


def test_getall():
    res = registry.getall()
    expected = ["cols", "idxcols"]
    assert sorted(res) == expected
    for col_t in expected:
        curdir = Path(resource_filename("friendly_data_registry", col_t))
        schemas = list(
            chain.from_iterable(curdir.glob(f"*.{fmt}") for fmt in ("json", "yaml"))
        )
        assert len(res[col_t]) == len(schemas)
