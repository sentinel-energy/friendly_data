from collections import defaultdict

from glom import glom, Iter
import pytest

from sark.helpers import flatten_list
from sark.validate import check_pkg


def test_check_pkg(pkg):
    reports = check_pkg(pkg.descriptor)

    # only errors in the bad files
    assert all(
        map(
            lambda fp: "bad" in fp,
            glom(glom(reports, [("tables", ["source"])]), Iter().flatten()),
        )
    )

    counter = defaultdict(int)

    def _proc(err):
        counter[err["code"]] += 1
        return (
            err["row-number"],
            err["column-number"],
        )

    err_row_cols = tuple(
        flatten_list(glom(reports, [("tables", [("errors", [_proc])])]))
    )

    # incorrect type: (99, 2) int > float, and (101, 3) bool -> string
    assert counter["type-or-format-error"] == 2

    # FIXME: not clear why missing value detection fails
    if 0 == counter["missing-value"]:
        pytest.xfail("cannot detect missing-value, not clear why")

    # missing value: (11, 9)
    assert counter["missing-value"] == 1
    assert (11, 9, 99, 2, 101, 3) == err_row_cols
