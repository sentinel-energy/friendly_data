"""Helpers"""

from importlib import import_module


def import_from(module: str, name: str):
    return getattr(import_module(module), name)
