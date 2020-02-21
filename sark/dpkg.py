"""Data package

PS: the coincidential module name is intentional ;)

"""

import json
from pathlib import Path
from typing import Dict, Iterable, Union

from datapackage import Package, Resource
from glom import glom
import pandas as pd

from sark.helpers import import_from

# TODO: compressed files
_source_ts = ["csv", "xls", "xlsx", "sqlite"]
_pd_types = {
    "boolean": "bool",
    # "date": "datetime64",
    # "time": "datetime64",
    "datetime": "datetime64",
    "integer": "int",
    "number": "float",
    "string": "string",
}


def create_pkg(meta: Dict, resources: Iterable[Union[str, Path, Dict]]):
    # for an interesting discussion on type hints with unions, see:
    # https://stackoverflow.com/q/60235477/289784
    pkg = Package(meta)
    for res in resources:
        if isinstance(res, (str, Path)):
            if not Path(res).exists():  # pragma: no cover, bad path
                continue
            pkg.infer(f"{res}")
        else:  # pragma: no cover, adding with Dict
            pkg.add_resource(res)
    return pkg


def read_pkg(pkg_json_path: Union[str, Path]):
    with open(pkg_json_path) as pkg_json:
        base_path = f"{Path(pkg_json_path).parent}"
        return Package(json.load(pkg_json), base_path=base_path)


def _source_type(source: Union[str, Path]):
    # FIXME: use file magic
    source_t = Path(source).suffix.strip(".").lower()
    if source_t not in _source_ts:
        raise ValueError(f"unsupported source: {source_t}")
    return source_t


def _schema(resource: Resource, type_map: Dict[str, str]) -> Dict[str, str]:
    return dict(
        glom(
            resource,  # target
            (  # spec
                "schema.fields",  # Resource & Schema properties
                [  # fields inside a list
                    (
                        "descriptor",  # Field property
                        lambda t: (  # str -> dtypes understood by pandas
                            t["name"],
                            type_map[t["type"]]
                            # (_type_d[t["type"]], t["format"]),
                        ),
                    )
                ],
            ),
        )
    )


def to_df(resource: Resource) -> pd.DataFrame:
    """"Reads a data package resource as a `pandas.DataFrame`

    FIXME: only considers 'name' and 'type' in the schema, other options like
    'format', 'missingValues', etc are ignored.

    Parameters
    ----------
    resource : `datapackage.Resource`
        A data package resource object

    Returns
    -------
    `pandas.DataFrame`

    Raises
    ------
    `ValueError`
        If the source type the resource is pointing to isn't supported

    """
    pd_readers = {
        "csv": "read_csv",
        "xls": "read_excel",
        "xlsx": "read_excel",
        "sqlite": "read_sql",
    }
    reader = import_from("pandas", pd_readers[_source_type(resource.source)])

    # parse dates
    schema = _schema(resource, _pd_types)
    date_cols = [col for col, col_t in schema.items() if "datetime64" in col_t]
    tuple(map(schema.pop, date_cols))

    # TODO: set index_col if 'required' is present
    return reader(resource.source, dtype=schema, parse_dates=date_cols)
