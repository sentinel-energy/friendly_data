"""Metadata tools"""

import json
import logging
from typing import Dict, Tuple

from sark.io import HttpCache

logger = logging.getLogger()

# Open Definition License Service, url template
ODLS = "https://licenses.opendefinition.org/licenses/groups/{}.json"
# ODLS groups: all, OSI compliant, Open Definition compliant, specially
# selected for CKAN (https://ckan.org)
ODLS_GROUPS = ["all", "osi", "od", "ckan"]

# type aliases
License = Dict[str, str]


def get_license(lic: str, group: str = "all") -> License:
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
    Dict[str, str], alias License
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
    if lic is None:
        lic_meta = _get_license_interactively(licenses, group)
    else:
        lic_meta = licenses[lic]
    return check_license(lic_meta)


def _get_license_interactively(
    licenses: Dict[str, License], group: str
) -> License:
    """Interactively ask for the license name to retrieve

    Parameters
    ----------
    licenses
        Dictionary of licenses
    group
        License group (for logging)

    Returns
    -------
    Dict[str, str], alias License
        License metadata
    """
    while True:
        lic = input("license: ")
        if lic not in licenses:
            logger.error(f"cannot find '{lic}' in license group '{group}'")
            logger.error("Press Ctrl-c to abort")
            continue
        return licenses[lic]


def check_license(lic_meta: License):
    """Return the license spec from the metadata

    Issue a warning if the license is old.  TODO: add other recommendations

    Parameters
    ----------
    lic_meta
        License metadata dictionary (as returned by the Open Definition
        License Service)
        Example: CC-BY-SA
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
    if lic_meta["status"] == "retired":
        logger.warning("You have picked a license that has been superseded!")
    return {
        "name": lic_meta["id"],
        "path": lic_meta["url"],
        "title": lic_meta["title"],
    }
