from copy import deepcopy
import json
from pathlib import Path

from glom import glom, Iter
import pandas as pd
import pandas.testing as tm
import pytest  # noqa: F401

from friendly_data.dpkg import read_pkg
from friendly_data.helpers import consume
from friendly_data.validate import check_pkg, check_schema, summarise_errors


def test_check_pkg():
    pkgdir = Path("testing/files/random-bad")
    pkg = read_pkg(pkgdir / "datapackage.json")
    # errors:
    #            filename  row  col              error
    # 0      mini-bad.csv   12              extra-cell
    # 1      mini-bad.csv   22  SRB         type-error
    # 2  sample-bad-1.csv   10  VBN   constraint-error
    # 3  sample-bad-1.csv   22  QWE         type-error
    # 4  sample-bad-1.csv   23  MPQ       missing-cell
    # 5  sample-bad-1.csv   24  RTY         type-error
    # 6  sample-bad-2.csv    7  IUY   constraint-error
    # 7  sample-bad-2.csv    8  APO   constraint-error
    # 8  sample-bad-2.csv    8       primary-key-error
    # 9  sample-bad-2.csv   10       primary-key-error

    report = check_pkg(pkg)
    # only errors in files w/ bad data
    assert glom(report, (Iter().map(lambda f: "bad" in f["path"]).all(), all))

    summary = summarise_errors(report)
    errcount = summary[["filename", "error"]].groupby("error").count()
    assert errcount.sum()[0] == 10  # total errors

    expected = pd.DataFrame(
        [
            ("constraint-error", 3),
            ("extra-cell", 1),
            ("missing-cell", 1),
            ("primary-key-error", 2),
            ("type-error", 3),
        ],
        columns=["error", "filename"],
    )
    tm.assert_frame_equal(errcount.reset_index(), expected)


def test_check_schema():
    pkgdir = Path("testing/files/random")
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
