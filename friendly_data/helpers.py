"""Collection of helper functions"""

from collections import deque
from collections.abc import Sequence
from functools import partial
from importlib import import_module
import re
import sys
from typing import Iterable

from glom import Check, Match, SKIP


def import_from(module: str, name: str):
    """Import ``name`` from ``module``."""
    return getattr(import_module(module), name)


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


class noop_map(dict):
    """A noop mapping class

    A dictionary subclass that falls back to noop on ``KeyError`` and returns
    the key being looked up.

    """

    def __missing__(self, key):
        return key


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

    """
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

    """
    return Match(pattern, default=SKIP, **kwargs)


consume = partial(deque, maxlen=0)
consume.__doc__ = "Consume or exhaust an iterator"
