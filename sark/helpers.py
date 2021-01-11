"""Helpers"""

from collections import deque
from collections.abc import Sequence
from functools import partial
from importlib import import_module
from typing import Callable, get_args, get_origin, get_type_hints, Iterable, Tuple

from glom import Check, Match, SKIP


def import_from(module: str, name: str):
    return getattr(import_module(module), name)


def from_hints(fn: Callable, arg: str) -> Tuple:
    hint = get_type_hints(fn)[arg]
    return get_origin(hint), get_args(hint)


def flatten_list(lst: Iterable) -> Iterable:
    """Flatten an arbitrarily nested list (returns a generator)"""
    for el in lst:
        if isinstance(el, Sequence) and not isinstance(el, (str, bytes)):
            yield from flatten_list(el)
        else:
            yield el


def select(spec, **kwargs):
    """Wrap `glom.Check` with the default action set to `glom.SKIP`.

    This is very useful to select items inside nested data structures.  A few
    example uses:

    >>> from glom import glom
    >>> cols = [
    ...     {"name": "abc", "type": "integer"},
    ...     {"name": "def", "type": "string"},
    ... ]
    >>> glom(cols, [select("name", equal_to="abc")])
    [{"name": "abc", "type": "integer"}]

    Full documentation of `glom.Check` is below:

    """
    return Check(spec, default=SKIP, **kwargs)


select.__doc__ += Check.__doc__


def match(pattern, **kwargs):
    """Wrap `glom.Match` with the default action set to `glom.SKIP`.

    This is very useful to match items inside nested data structures.  A few
    example uses:

    >>> from glom import glom
    >>> cols = [
    ...     {"name": "abc", "type": "integer", "constraints": {"enum": []}},
    ...     {"name": "def", "type": "string"},
    ... ]
    >>> glom(cols, [match({"constraints": {"enum": list}, str: str})])
    [{"name": "abc", "type": "integer", "constraints": {"enum": []}}]

    Full documentation of `glom.Match` is below:

    """
    return Match(pattern, default=SKIP, **kwargs)


match.__doc__ += Match.__doc__


consume = partial(deque, maxlen=0)
consume.__doc__ = "Consume or exhaust an iterator"
