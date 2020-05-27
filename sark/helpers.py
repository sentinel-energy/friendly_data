"""Helpers"""

from collections import deque, Sequence
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
consume = partial(deque, maxlen=0)
