from typing import Dict

from jinja2 import BaseLoader, Environment, FileSystemLoader
from pkg_resources import resource_filename

from friendly_data import logger_config
import friendly_data_registry as registry

logger = logger_config(fmt="{name}: {levelname}: {message}")


def template_from_str(template: str):
    return Environment(loader=BaseLoader()).from_string(template)


def get_template(name: str):
    loader = FileSystemLoader(searchpath=resource_filename("friendly_data", "doc"))
    env = Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
    return env.get_template(name)


def entry(schema: Dict, f: str, markup: str = "rst") -> str:
    return get_template(f"entry.{markup}.template").render({"file": f, **schema})


def page(*, markup: str = "rst", col_t: str = "") -> str:
    code = "``" if markup == "rst" else "`"
    col_types = {
        "cols": f"Value columns - {code}cols{code}",
        "idxcols": f"Index columns - {code}idxcols{code}",
    }
    if col_t == "cols":
        col_types.pop("idxcols")
    elif col_t == "idxcols":
        col_types.pop("cols")
    elif col_t:
        logger.warning(f"{col_t}: unknown column type, will return all")
    contents = [
        (col_types[_col_t], [entry(schema, f, markup) for schema, f in schemas])
        for _col_t, schemas in registry.getall(with_file=True).items()
        if _col_t in col_types
    ]
    return get_template(f"page.{markup}.template").render({"sections": contents})
