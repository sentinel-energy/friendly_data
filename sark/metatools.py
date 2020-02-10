"""Metadata tools"""

import json
import logging
from typing import Dict

from .io import HttpCache

logger = logging.getLogger()

# Open Definition License Service, url template
ODLS = "https://licenses.opendefinition.org/licenses/groups/{}.json"
# ODLS groups: all, OSI compliant, Open Definition compliant, specially
# selected for CKAN (https://ckan.org)
ODLS_GROUPS = ["all", "osi", "od", "ckan"]


def get_license(lic: str, group: str = "all") -> Dict[str, str]:
    cache = HttpCache(ODLS)
    if group not in ODLS_GROUPS:
        raise ValueError(
            f"unknown license group: {group},"
            f" should be one of: {ODLS_GROUPS}"
        )
    licenses = json.loads(cache.get(group))
    return licenses[lic]
