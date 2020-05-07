from collections import defaultdict

from glom import glom, Iter
import pytest

from sark.dpkg import create_pkg
from sark.helpers import flatten_list
from sark.metatools import get_license
from sark.validate import check_pkg


def test_check_pkg(pkgdir, subtests):
    datadir = pkgdir / "data"
    pkg_meta = {"name": "test", "licenses": get_license("CC0-1.0")}
    pkg = create_pkg(pkg_meta, datadir.glob("*.csv"))

    reports = check_pkg(pkg.descriptor)

    with subtests.test(msg="detect bad files"):
        # only errors in the bad files
        assert all(
            map(
                lambda fp: "bad" in fp,
                glom(
                    glom(reports, [("tables", ["source"])]), Iter().flatten()
                ),
            )
        )

    counter = defaultdict(int)

    def _proc(err):
        counter[err["code"]] += 1
        return (
            err["row-number"],
            err["column-number"],
        )

    with subtests.test(msg="type/format error"):
        err_row_cols = tuple(
            flatten_list(glom(reports, [("tables", [("errors", [_proc])])]))
        )

        # incorrect type: (99, 2) int > float, and (101, 3) bool -> string
        assert counter["type-or-format-error"] == 2

    with subtests.test(msg="missing values"):
        pass

    # FIXME: not clear why missing value detection fails
    if 0 == counter["missing-value"]:
        pytest.xfail("cannot detect missing-value, not clear why")

    # missing value: (11, 9)
    assert counter["missing-value"] == 1
    assert (11, 9, 99, 2, 101, 3) == err_row_cols
