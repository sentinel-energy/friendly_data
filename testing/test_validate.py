from collections import defaultdict
from copy import deepcopy
import json

from glom import Assign, glom, Iter, T
import pytest  # noqa: F401

from sark.dpkg import create_pkg
from sark.helpers import flatten_list, consume, select
from sark.metatools import get_license
from sark.validate import check_pkg, check_schema, summarise_errors


def test_check_pkg(pkgdir):
    pkg_meta = {"name": "test", "licenses": get_license("CC0-1.0")}
    csvs = [f.relative_to(pkgdir) for f in (pkgdir / "data").glob("*.csv")]
    pkg = create_pkg(pkg_meta, csvs, base_path=str(pkgdir))
    # mark column VBN as required in sample-bad.csv
    glom(
        pkg.descriptor,
        (
            "resources",
            Iter().filter(select(T["name"], equal_to="sample-bad")).first(),
            "schema.fields",
            Iter().filter(select(T["name"], equal_to="VBN")).first(),
            Assign("constraints", {"required": True}),
        ),
    )
    pkg.commit()

    reports = check_pkg(pkg)

    # only errors in files w/ bad data
    assert all(
        map(
            lambda fp: "bad" in fp,
            glom(reports, ([("tables", ["source"])], Iter().flatten())),
        )
    )

    counter = defaultdict(int)

    def _proc(err):
        counter[err["code"]] += 1
        return err["row-number"], err["column-number"]

    # extract cell numbers with errors
    err_row_cols = tuple(
        flatten_list(glom(reports, [("tables", [("errors", [_proc])])]))
    )

    # incorrect type: (99, 2) int -> float, and (101, 3) bool -> string
    assert counter["type-or-format-error"] == 2

    # required but absent (NA): (11, 9)
    assert counter["required-constraint"] == 1

    # missing value (cell missing, not just empty): (100, 10)
    assert counter["missing-value"] == 1

    # match cells numbers with errors
    assert (11, 9, 99, 2, 100, 10, 101, 3) == err_row_cols

    assert not summarise_errors(reports).empty


def test_check_schema(pkgdir):
    with open(pkgdir / "schemas/sample-ok-1.json") as json_file:
        schema = json.load(json_file)

    ref = deepcopy(schema)
    consume(map(ref["fields"].pop, [-2] * 3))  # drop 'VBN', 'ZXC', and 'JKL'

    schema_bad = deepcopy(schema)
    schema_bad["fields"][-2]["name"] = "FJW"  # rename 'VBN', no error
    schema_bad["fields"][4]["name"] = "WOP"  # rename 'ASD', missing column
    schema_bad["primaryKey"] = "time"  # ["time"] -> "time", no error

    status, missing, mismatch, pri = check_schema(ref, schema_bad)
    assert status is False
    assert missing == {"ASD"}
    assert pri == []

    status, missing, mismatch, _ = check_schema(ref, schema_bad, remap={"WOP": "ASD"})
    assert status is True
    assert missing == set()

    # 'time': 'datetime' -> 'string', mismatching type
    schema_bad["fields"][0]["type"] = "string"
    # 'QWE': 'integer' -> 'number', mismatching type
    schema_bad["fields"][1]["type"] = "number"

    assert check_schema(ref, schema) == (True, set(), dict(), list())

    status, missing, mismatch, _ = check_schema(ref, schema_bad)
    assert status is False
    assert mismatch.get("time") == ("datetime", "string")
    assert mismatch.get("QWE") == ("integer", "number")

    with open(pkgdir / "schemas/sample-ok-2.json") as json_file:
        schema = json.load(json_file)

    ref = deepcopy(schema)
    schema_bad = deepcopy(schema)
    # drop 4th level from: ["lvl", "TRE", "IUY", "APO"]
    idxlvls = schema["primaryKey"]
    schema_bad["primaryKey"] = idxlvls[:-1]
    status, missing, mismatch, pri = check_schema(ref, schema_bad)
    assert status is False
    assert pri == [(3, "APO", None)]

    schema_bad["primaryKey"] = idxlvls[:2] + idxlvls[3:]  # drop 3rd level
    status, missing, mismatch, pri = check_schema(ref, schema_bad)
    assert status is False
    assert pri == [(2, "IUY", "APO"), (3, "APO", None)]
