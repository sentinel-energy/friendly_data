from itertools import chain
import logging
from pathlib import Path

from glom import glom, Iter, MatchError, TypeMatchError
from pkg_resources import resource_filename
import pytest

import friendly_data.registry as registry
import friendly_data_registry as _registry

from friendly_data.helpers import match

from .conftest import assert_log, custom_registry


@pytest.mark.parametrize("reg", [registry, _registry])
@pytest.mark.parametrize("col, col_t", [("region", "idxcols"), ("storage", "cols")])
def test_registry(col, col_t, reg):
    res = reg.get(col, col_t)
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


@pytest.mark.parametrize("reg", [_registry, registry])
def test_getall(reg):
    res = reg.getall()
    expected = ["cols", "idxcols"]
    assert sorted(res) == expected
    for col_t in expected:
        curdir = Path(resource_filename("friendly_data_registry", col_t))
        schemas = list(
            chain.from_iterable(curdir.glob(f"*.{fmt}") for fmt in ("json", "yaml"))
        )
        assert len(res[col_t]) == len(schemas)


def test_registry_schema_validation(caplog):
    conf = custom_registry()
    schema = registry.RegistrySchema(conf)
    assert "idxcols" in schema and "cols" in schema
    assert schema["idxcols"] and schema["cols"]

    conf["bad_cols"] = conf.pop("cols")
    with pytest.raises(MatchError):
        registry.RegistrySchema(conf)
    assert_log(caplog, "bad key in schema", "ERROR")

    conf["cols"] = conf.pop("bad_cols")[0]
    with pytest.raises(TypeMatchError):
        registry.RegistrySchema(conf)
    assert_log(caplog, "type mismatch", "ERROR")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"confdict": custom_registry()},
        {"conffile": "testing/files/custom_registry/config.yaml"},
    ],
)
def test_custom_registry(kwargs):
    with registry.config_ctx(**kwargs) as c:
        assert c == custom_registry()

        # new column
        res = registry.get("enduse", "idxcols")
        assert len(res["constraints"]["enum"])

        # update existing column
        res = registry.get("capacity_factor", "cols")
        assert glom(res, "constraints.maximum") == 100

        res = registry.getall()
        assert len(res) == 2
        assert glom(res, ("idxcols", Iter("name").filter(match("enduse")).all()))

    # existing column default value
    res = registry.get("capacity_factor", "cols")
    assert glom(res, "constraints.maximum") == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"idxcols": custom_registry()["idxcols"]},
        {"cols": custom_registry()["idxcols"]},
    ],
)
def test_custom_registry_partial(kwargs):
    with registry.config_ctx(**kwargs) as c:
        kwd, *_ = kwargs.keys()
        assert c[kwd] == custom_registry()["idxcols"]

        res = registry.get("enduse", kwd)
        assert len(res["constraints"]["enum"])

        res = registry.getall()
        assert len(res) == 2
        assert glom(res, (kwd, Iter("name").filter(match("enduse")).all(), len)) == 1


def test_custom_registry_errors(caplog):
    conf = custom_registry()
    conf["bad_cols"] = conf.pop("cols")
    with registry.config_ctx(confdict=conf) as c:
        assert c == {"idxcols": [], "cols": []}
    assert_log(caplog, "ignoring bad custom registry", "ERROR")
