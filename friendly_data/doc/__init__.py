from typing import Dict

from jinja2 import Environment, FileSystemLoader
from pkg_resources import resource_filename

import friendly_data_registry as registry


def get_template(name: str):
    loader = FileSystemLoader(
        searchpath=resource_filename("friendly_data_registry", "doc")
    )
    env = Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
    return env.get_template(name)


def entry(schema: Dict, f: str) -> str:
    return get_template("entry.rst.template").render({"file": f, **schema})


def page() -> str:
    col_types = {
        "cols": "Value columns - ``cols``",
        "idxcols": "Index columns - ``idxcols``",
    }
    contents = [
        (col_types[col_t], [entry(schema, f) for schema, f in schemas])
        for col_t, schemas in registry.getall(with_file=True).items()
    ]
    return get_template("page.rst.template").render({"sections": contents})
