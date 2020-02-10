"""Metadata tools"""

import json
import logging
from typing import Dict

from sark.io import HttpCache

logger = logging.getLogger()

# Open Definition License Service, url template
ODLS = "https://licenses.opendefinition.org/licenses/groups/{}.json"
# ODLS groups: all, OSI compliant, Open Definition compliant, specially
# selected for CKAN (https://ckan.org)
ODLS_GROUPS = ["all", "osi", "od", "ckan"]


def get_license(lic: str, group: str = "all") -> Dict[str, str]:
    """Return the license metadata

    Retrieve the license metadata of the requested group from the Open
    Definition License Service and cache it in a temporary file.  From the
    retrieved list, find the requested license and return it.

    Parameters
    ----------
    lic : str
        Requested license
    group : {'all', 'osi', 'od', 'ckan'}
        License group where to find the license

    Returns
    -------
    Dict[str, str]
        A dictionary with the license metadata

    Raises
    ------
    ValueError
        If the license group is incorrect
    KeyError
        If the license cannot be found in the provided group

    """

    if group not in ODLS_GROUPS:
        raise ValueError(
            f"unknown license group: {group},"
            f" should be one of: {ODLS_GROUPS}"
        )
    cache = HttpCache(ODLS)
    licenses = json.loads(cache.get(group))
    return licenses[lic]
