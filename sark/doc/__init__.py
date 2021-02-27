from jinja2 import Environment, FileSystemLoader
from pkg_resources import resource_filename

import sark_registry as registry


def get_template(name: str):
    loader = FileSystemLoader(searchpath=resource_filename("sark", "doc"))
    env = Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
    return env.get_template(name)


def entry(schema) -> str:
    return get_template("entry.rst.template").render(schema)


def page() -> str:
    col_types = {
        "cols": "Value columns - ``cols``",
        "idxcols": "Index columns - ``idxcols``",
    }
    contents = [
        (col_types[col_t], [entry(schema) for schema in schemas])
        for col_t, schemas in registry.getall().items()
    ]
    return get_template("page.rst.template").render({"sections": contents})
