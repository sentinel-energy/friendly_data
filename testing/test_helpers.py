from typing import Callable

import pytest

from friendly_data.helpers import import_from

from .conftest import assert_log


def test_import_from(caplog):
    pd_read_csv = import_from("pandas", "read_csv")
    assert isinstance(pd_read_csv, Callable)

    fakemodule = "notthere"
    with pytest.raises(ImportError, match=f".'{fakemodule}'"):
        import_from(fakemodule, "")
    assert_log(caplog, "use pip or conda to install", "ERROR")
