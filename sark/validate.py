"""Data resource validation

Tools to check if a data resource is consistent with its schema

"""

from typing import Callable, Dict

from glom import glom, SKIP
from goodtables import validate


def check_pkg(pkg_desc: Dict, _filter: Callable = lambda res: True) -> Dict:
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
