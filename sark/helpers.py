"""Helpers"""

from importlib import import_module


def import_from(module, name):
    return getattr(import_module(module), name)
