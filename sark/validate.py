"""Data resource validation

Tools to check if a data resource is consistent with its schema

"""

from copy import deepcopy
from typing import Callable, Dict, List, Set, Tuple, Union

from glom import glom, SKIP, T
from goodtables import validate

from sark.helpers import select


def check_pkg(pkg_desc: Dict, _filter: Callable = lambda res: True) -> Dict:
    """Validate all resources in a datapackage

    Parameters
    ----------
    pkg_desc : Dict
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
    validation_spec = (
        "resources",
        [
            lambda res: validate(res["path"], schema=res["schema"])
            if _filter(res)
            else SKIP
        ],
    )
    reports = glom(pkg_desc, validation_spec)
    return glom(
        reports, [lambda report: report if report["error-count"] > 0 else SKIP]
    )


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
       Column/field name remapping (ignored for now)

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
    schema = (deepcopy(ref), deepcopy(dst))  # work with copies

    # extract columns
    ref_: List[Dict[str, str]]
    dst_: List[Dict[str, str]]
    ref_, dst_ = glom(schema, [T.pop("fields")])

    # column names
    ref_set: Set[str]
    dst_set: Set[str]
    ref_set, dst_set = glom((ref_, dst_), [(["name"], set)])

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
