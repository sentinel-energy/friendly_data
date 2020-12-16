"""Helpers"""

from collections import deque
from collections.abc import Sequence
from functools import partial
from importlib import import_module
from typing import Iterable

from glom import Check, SKIP


def import_from(module: str, name: str):
    return getattr(import_module(module), name)


def flatten_list(lst: Iterable) -> Iterable:
    """Flatten an arbitrarily nested list (returns a generator)"""
    for el in lst:
        if isinstance(el, Sequence) and not isinstance(el, (str, bytes)):
            yield from flatten_list(el)
        else:
            yield el


select = partial(Check, default=SKIP)
select.__doc__ = f"""
Wrap `glom.Check` object with the default action set to `glom.SKIP`.

This is very useful to select items inside nested data structures.  A few
example uses:

>>> from glom import glom
>>> cols = [
...     {{"name": "abc", "type": "integer"}},
...     {{"name": "def", "type": "string"}},
... ]
>>> select(cols, [select("name", equal_to="abc")])
[{{"name": "abc", "type": "integer"}}]

Full documentation of `glom.Check` is below:

{Check.__doc__}
"""

consume = partial(deque, maxlen=0)
consume.__doc__ = "Consume or exhaust an iterator"
