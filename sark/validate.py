"""Data resource validation

Tools to check if a data resource is consistent with its schema

"""

from typing import Callable, Dict

from glom import glom, SKIP
from goodtables import validate


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
