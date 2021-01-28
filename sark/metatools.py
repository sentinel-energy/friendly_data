"""Metadata tools"""

import json
import logging
from operator import contains
from typing import Dict, List, Tuple

from sark.io import HttpCache
from sark._types import _license_t

logger = logging.getLogger()

# Open Definition License Service, url template
ODLS = "https://licenses.opendefinition.org/licenses/groups/{}.json"
# ODLS groups: all, OSI compliant, Open Definition compliant, specially
# selected for CKAN (https://ckan.org)
ODLS_GROUPS = ["all", "osi", "od", "ckan"]


def _fetch_license(group: str = "all") -> Dict:
    if group not in ODLS_GROUPS:
        raise ValueError(
            f"unknown license group: {group}, should be one of: {ODLS_GROUPS}"
        )
    cache = HttpCache(ODLS)
    return json.loads(cache.get(group))


def list_licenses(group: str = "all") -> List[str]:
    """Return list of valid licenses"""
    return list(_fetch_license(group).keys())


def get_license(lic: str, group: str = "all") -> _license_t:
    """Return the license metadata

    Retrieve the license metadata of the requested group from the Open
    Definition License Service and cache it in a temporary file.  From the
    retrieved list, find the requested license and return it.

    Parameters
    ----------
    lic : str or None
        Requested license; if None, interactively ask for the license name
    group : {'all', 'osi', 'od', 'ckan'}
        License group where to find the license

    Returns
    -------
    Dict[str, str], alias _license_t
        A dictionary with the license metadata

    Raises
    ------
    ValueError
        If the license group is incorrect
    KeyError
        If the license cannot be found in the provided group

    """

    licenses = _fetch_license(group)
    return check_license(licenses[lic])


def check_license(lic: _license_t) -> _license_t:
    """Return the license spec from the metadata

    Issue a warning if the license is old.  TODO: add other recommendations

    Parameters
    ----------
    lic : Dict[str, str], alias _license_t
        License metadata dictionary (as returned by the Open Definition
        License Service)
        Example: CC-BY-SA::

          {
            "domain_content": true,
            "domain_data": true,
            "domain_software": false,
            "family": "",
            "id": "CC-BY-SA-4.0",
            "maintainer": "Creative Commons",
            "od_conformance": "approved",
            "osd_conformance": "not reviewed",
            "status": "active",
            "title": "Creative Commons Attribution Share-Alike 4.0",
            "url": "https://creativecommons.org/licenses/by-sa/4.0/"
          }

    """
    # TODO: to add more checks, add to the following lists
    for check, op, ref in zip(
        [_license_status, _license_domain],  # check
        [contains, contains],  # test
        ["active", "data"],  # reference
    ):
        if not op(check(lic), ref):
            logger.warning(f"inappropriate license: not {ref}")
    return {
        "name": lic["id"],
        "path": lic["url"],
        "title": lic["title"],
    }


def _license_status(lic: _license_t) -> str:
    return lic["status"]


def _license_domain(lic: _license_t) -> Tuple[str, ...]:
    return tuple(
        lic_t for lic_t in ["content", "data", "software"] if lic[f"domain_{lic_t}"]
    )
