"""Collection of helper functions"""

from collections import deque
from collections.abc import Sequence
from functools import partial
from importlib import import_module
from logging import getLogger
import re
import sys
from typing import Dict, Iterable, List, Tuple

from glom import Check, Match, SKIP
import pandas as pd

logger = getLogger(__name__)


def import_from(module: str, name: str):
    """Import ``name`` from ``module``, if ``name`` is empty, return module"""
    try:
        mod = import_module(module)
    except ImportError as err:
        msg = f"Missing optional dependency '{module}', use pip or conda to install"
        logger.error(msg)
        raise err from None
    else:
        return getattr(mod, name) if name else mod


def is_windows() -> bool:
    """Check if we are on Windows"""
    return sys.platform in ("win32", "cygwin")


def sanitise(string: str) -> str:
    """Sanitise string for use as group/directory name"""
    return "_".join(re.findall(re.compile("[^ @&()/]+"), string))


def is_fmtstr(string: str) -> bool:
    opening_braces = string.count("{")
    closing_braces = string.count("}")
    return bool(opening_braces and closing_braces and opening_braces == closing_braces)


# def from_hints(fn: Callable, arg: str) -> Tuple:
#     """NOTE: Comment out until we drop 3.7"""
#     from typing import get_args, get_origin, get_type_hints

#     hint = get_type_hints(fn)[arg]
#     return get_origin(hint), get_args(hint)


def flatten_list(lst: Iterable) -> Iterable:
    """Flatten an arbitrarily nested list (returns a generator)"""
    for el in lst:
        if isinstance(el, Sequence) and not isinstance(el, (str, bytes)):
            yield from flatten_list(el)
        else:
            yield el


def filter_dict(data: Dict, allowed: Iterable) -> Dict:
    """Filter a dictionary based on a set of allowed keys"""
    return dict(filter(lambda kv: kv[0] in allowed, data.items()))


class noop_map(dict):
    """A noop mapping class

    A dictionary subclass that falls back to noop on ``KeyError`` and returns
    the key being looked up.

    """

    def __missing__(self, key):
        return key


def idx_lvl_values(idx: pd.MultiIndex, name: str) -> pd.Index:
    """Given a ``pandas.MultiIndex`` and a level name, find the level values

    Parameters
    ----------
    idx : pandas.MultiIndex
        A multi index

    name : str
        Level name

    Returns
    -------
    pandas.Index
        Index with the level values

    """
    return idx.levels[idx.names.index(name)]


def idxslice(lvls: Iterable[str], selection: Dict[str, List]) -> Tuple:
    """Create an index slice tuple from a set of level names, and selection mapping

    NOTE: The order of ``lvls`` should match the order of the levels in the
    index exactly; typically, ``mydf.index.names``.

    Parameters
    ----------
    lvls : Iterable[str]
        Complete set of levels in the index

    selection : Dict[str, List]
        Selection set; the key is a level name, and the value is a list of
        values to select

    Returns
    -------
    Tuple
        Tuple of values, with ``slice(None)`` for skipped levels (matches anything)

    """
    return tuple(selection[lvl] if lvl in selection else slice(None) for lvl in lvls)


def select(spec, **kwargs):
    """Wrap ``glom.Check`` with the default action set to ``glom.SKIP``.

    This is very useful to select items inside nested data structures.  A few
    example uses:

    >>> from glom import glom
    >>> cols = [
    ...     {
    ...         "name": "abc",
    ...         "type": "integer"
    ...     },
    ...     {
    ...         "name": "def",
    ...         "type": "string"
    ...     },
    ... ]
    >>> glom(cols, [select("name", equal_to="abc")])
    [{"name": "abc", "type": "integer"}]

    For details see: `glom.Check`_

    .. _glom.Check: https://glom.readthedocs.io/en/latest/matching.html#validation-with-check

    """  # noqa: E501
    return Check(spec, default=SKIP, **kwargs)


def match(pattern, **kwargs):
    """Wrap ``glom.Match`` with the default action set to ``glom.SKIP``.

    This is very useful to match items inside nested data structures.  A few
    example uses:

    >>> from glom import glom
    >>> cols = [
    ...     {
    ...         "name": "abc",
    ...         "type": "integer",
    ...         "constraints": {"enum": []}
    ...     },
    ...     {
    ...         "name": "def",
    ...         "type": "string"
    ...     },
    ... ]
    >>> glom(cols, [match({"constraints": {"enum": list}, str: str})])
    [{"name": "abc", "type": "integer", "constraints": {"enum": []}}]

    For details see: `glom.Match`_

    .. _glom.Match: https://glom.readthedocs.io/en/latest/matching.html#validation-with-match

    """  # noqa: E501
    return Match(pattern, default=SKIP, **kwargs)


consume = partial(deque, maxlen=0)
consume.__doc__ = "Consume or exhaust an iterator"
