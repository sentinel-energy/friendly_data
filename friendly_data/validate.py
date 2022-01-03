"""Functions useful to validate a data package or parts of its schema.

"""

from typing import Callable, Dict, List, Set, Tuple

from frictionless import validate_package as validate
from glom import Coalesce, glom, Iter, T
import pandas as pd

from friendly_data.helpers import select


def check_pkg(pkg) -> List[Dict]:
    """Validate all resources in a datapackage for common errors.

    Typical errors that are checked:
     - ``blank-header``,
     - ``extra-label``,
     - ``missing-label``,
     - ``blank-label``,
     - ``duplicate-label``,
     - ``incorrect-label``,
     - ``blank-row``,
     - ``primary-key-error``,
     - ``foreign-key-error``,
     - ``extra-cell``,
     - ``missing-cell``,
     - ``type-error``,
     - ``constraint-error``,
     - ``unique-error``

    Parameters
    ----------
    pkg : frictionless.Package
        The datapackage descriptor dictionary

    Returns
    -------
    Dict
        A dictionary with a summary of the validation checks.

    """
    # noinfer -> original in newer versions
    report = validate(pkg, basepath=pkg.basepath, noinfer=True)
    count = glom(report, "stats.errors")
    if not count:
        return list()
    res = glom(
        report,
        (
            "tasks",
            Iter()
            .filter(T["stats"]["errors"])
            .map(
                {
                    "path": T["resource"]["path"],
                    "position": (
                        T["errors"],
                        [
                            {
                                "row": T["rowNumber"],
                                "col": Coalesce(T["fieldName"], default=""),
                            }
                        ],
                    ),
                    "errors": (
                        T["errors"],
                        [
                            {
                                "error": T["code"],
                                "remark": T["note"],
                            }
                        ],
                    ),
                }
            )
            .all(),
        ),
    )
    return res


def summarise_errors(report: List[Dict]) -> pd.DataFrame:
    """Summarise the dict/json error report as a `pandas.DataFrame`

    Parameters
    ----------
    report : List[Dict]
        List of errors as returned by :func:`check_pkg`

    Returns
    -------
    pandas.DataFrame
        Summary dataframe; example::

               filename  row  col       error  remark
            0   bad.csv   12       extra-cell     ...
            1   bad.csv   22  SRB  type-error     ...

    """
    df = pd.DataFrame(report)
    errors: pd.DataFrame = df["errors"].explode(ignore_index=True).apply(pd.Series)
    df = df.explode("position").reset_index(drop=True).drop("errors", axis=1)
    fnames: pd.DataFrame = df["path"].str.rsplit("/").apply(pd.Series).iloc[:, -1]
    fnames.name = "filename"
    position: pd.Series = df["position"].apply(pd.Series)
    return pd.concat([fnames, position, errors], axis=1)


def check_schema(
    ref: Dict[str, str], dst: Dict[str, str], *, remap: Dict[str, str] = None
) -> Tuple[bool, Set[str], Dict[str, Tuple[str, str]], List[Tuple]]:
    """Compare a schema with a reference.

    The reference schema is a minimal set, meaning, any additional fields in
    the compared schema are accepted, but omissions are not.

    Name comparisons are case-sensitive.

    TODO: maybe also compare constraints?

    Parameters
    ----------
    ref : Dict[str, str]
        Reference schema dictionary
    dst : Dict[str, str]
        Schema dictionary from the dataset being validated
    remap : Dict[str, str] (optional)
       Column/field names that are to be remapped before checking.

    Returns
    -------
    result : Tuple[bool, Set[str], Dict[str, Tuple[str, str]], List[Tuple]]
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

        - If primary keys are different, tuple with the diff.  The first
          element is the index where the two differ, and the two subsequent
          elements are the corresponding elements from the reference and
          dataset primary key list: ``(index, ref_col, dst_col)``

    """
    # extract columns
    ref_: List[Dict[str, str]]
    dst_: List[Dict[str, str]]
    ref_, dst_ = glom((ref, dst), Iter("fields").all())

    if remap:
        dst_ = [
            {**i, "name": remap[i["name"]] if i["name"] in remap else i["name"]}
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

    # metadata: ignore missing values
    pri_ref = ref.get("primaryKey", [])  # type: ignore
    pri_dst = dst.get("primaryKey", [])  # type: ignore
    if isinstance(pri_ref, str):
        pri_ref = [pri_ref]
    if isinstance(pri_dst, str):
        pri_dst = [pri_dst]

    def pair(i: List[str], j: List[str]) -> Callable[[], Tuple]:
        iitr, jitr = iter(i), iter(j)

        def _pair() -> Tuple:
            return next(iitr, None), next(jitr, None)

        return _pair

    pri_diff = []
    if pri_ref != pri_dst:
        pairs = iter(pair(pri_ref, pri_dst), (None, None))
        pri_diff = [(i, j, k) for i, (j, k) in enumerate(pairs) if j != k]

    check_pass = not (missing or mismatch or pri_diff)
    return (check_pass, missing, mismatch, pri_diff)  # type: ignore


def summarise_diff(
    diff: Tuple[bool, Set[str], Dict[str, Tuple[str, str]], List[Tuple]]
) -> str:
    """Summarise the schema diff from :func:`check_schema` results as a
    ``pandas.DataFrame``.

    """

    status, missing, mismatch, pri = diff
    report = ""
    if status:
        return report
    if missing:
        report += f"missing column names: {missing}\n"
    if mismatch:
        df = pd.DataFrame(
            [(col, *col_ts) for col, col_ts in mismatch.items()],
            columns=["column", "reference_type", "current_type"],
        )
        report += "mismatched column types:\n"
        report += str(df.to_string(header=True, index=False))
    if pri:
        df = pd.DataFrame(pri, columns=["level", "reference_col", "current_col"])
        report += "mismatched index levels/cols:\n"
        report += str(df.to_string(header=True, index=False))
    return report
