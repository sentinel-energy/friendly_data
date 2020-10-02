"""Data resource validation

Tools to check if a data resource is consistent with its schema

"""

from collections import defaultdict
from typing import Callable, Dict, List, Set, Tuple, Union

from glom import Fold, glom, Invoke, Iter, T
from goodtables import validate
import pandas as pd

from sark.helpers import select


def check_pkg(pkg, _filter: Callable = lambda res: True) -> List[Dict]:
    """Validate all resources in a datapackage

    Parameters
    ----------
    pkg : Package
        The datapackage descriptor dictionary
    _filter : Callable
        A predicate function that maybe passed to filter out data resources.
        The function is called with the data resource descriptor dictionary as
        argument, if it returns `False` the resource is skipped

    Returns
    -------
    Dict
        A dictionary with a summary of the validation checks.

    """
    return glom(
        pkg.resources,
        Iter()
        .filter(_filter)
        .map(Invoke(validate).specs(T.source, schema=T.schema.descriptor))
        .filter(lambda r: r["error-count"] > 0)
        .all(),
    )


def summarise_errors(reports: List[Dict]) -> Union[pd.DataFrame, None]:
    counts = lambda: defaultdict(lambda: dict(count=0, row=[], col=[]))

    def accumulate(res, err):
        stats = res[err["code"]]
        stats["count"] += 1
        stats["row"].append(err["row-number"])
        stats["col"].append(err["column-number"])
        return res

    summary = list(
        glom(
            reports,
            (
                [
                    (
                        "tables",
                        [
                            {
                                "source": T["source"].rsplit("/", 1)[-1],
                                "errors": (
                                    "errors",
                                    Fold(T, counts, accumulate),
                                    dict,
                                ),
                            }
                        ],
                    )
                ],
                Iter().flatten(),
            ),
        ),
    )
    # transform nested dict as records of dataframe
    rows = []
    for row in summary:
        for err, stats in row.pop("errors").items():
            _count = stats.pop("count")
            for i, j in zip(stats["row"], stats["col"]):
                rows += [
                    {**row, "error": err, "count": _count, "row": i, "col": j}
                ]
    return pd.DataFrame(rows).set_index(["source", "error"]) if rows else None


_cols = Union[Set, Set[str]]
_mismatch = Union[Dict, Dict[str, Tuple[str, str]]]


def check_schema(
    ref: Dict[str, str], dst: Dict[str, str], *, remap: Dict[str, str] = None
) -> Tuple[bool, _cols, _mismatch]:
    """Compare a schema with a reference.

    The reference schema is a minimal set, meaning, any additional fields in
    the compared schema are accepted, but omissions are not.

    Name comparisons are case-sensitive.

    TODO: name remappings will be supported in the future.

    NOTE: At the moment only name and types are compared

    Parameters
    ----------
    ref : Dict[str, str]
        Reference schema descriptor dictionary
    dst : Dict[str, str]
        Schema descriptor dictionary from the dataset being validated
    remap : Dict[str, str] (optional)
       Column/field names that are to be remapped before checking.

    Returns
    -------
    result : Tuple[bool, Union[Set, Set[str]], Union[Dict, Dict[str, Tuple[str, str]]]]
        Result tuple:

        - Boolean flag indicating if it passed the checks or not
        - If checks failed, set of missing columns from minimal set
        - If checks failed, set of columns with mismatching types.  It is a
          dictionary with the column name as key, and the reference type and
          the actual type in a tuple as value. ::

              {
                  'col_x': ('integer', 'number'),
                  'col_y': ('datetime', 'string'),
              }

    """
    # extract columns
    ref_: List[Dict[str, str]]
    dst_: List[Dict[str, str]]
    ref_, dst_ = glom((ref, dst), Iter("fields").all())

    if remap:
        dst_ = [
            {
                **i,
                "name": remap[i["name"]] if i["name"] in remap else i["name"],
            }
            for i in dst_
        ]

    # column names
    ref_set = glom(ref_, (["name"], set))
    dst_set = glom(dst_, (["name"], set))
    # missing columns
    missing = ref_set - dst_set

    # mismatched types, FIXME: horrible mess
    common = ref_set.intersection(dst_set)
    mismatch = {}
    for col in dst_:
        if col["name"] not in common:
            continue
        ref_col, *_ = glom(ref_, [select("name", equal_to=col["name"])])
        if ref_col["type"] != col["type"]:
            mismatch[col["name"]] = (ref_col["type"], col["type"])

    return (not (missing or mismatch), missing, mismatch)


def summarise_diff(diff: Tuple[bool, Set[str], Dict[str, Tuple[str, str]]]):
    status, missing, mismatch = diff
    report = ""
    if status:
        return report
    if missing:
        report += f"mismatched column names: {missing}\n"
    if mismatch:
        df = pd.DataFrame(
            [(col, *col_ts) for col, col_ts in mismatch.items()],
            columns=["column", "reference_type", "current_type"],
        )
        report += "mismatched column types:\n"
        report += str(df.to_string(header=True, index=False))
    return report
