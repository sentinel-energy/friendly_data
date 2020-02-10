"""Command line utilities"""

import re
from argparse import (
    ArgumentDefaultsHelpFormatter,
    RawDescriptionHelpFormatter,
)


class RawArgDefaultFormatter(
    ArgumentDefaultsHelpFormatter, RawDescriptionHelpFormatter
):
    """Combine raw help text formatting with default argument display."""

    pass


def sanitise(string: str) -> str:
    """Sanitise string for use as group/directory name"""
    return "_".join(re.findall(re.compile("[^ @&()/]+"), string))
