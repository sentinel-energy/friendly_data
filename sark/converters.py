from pathlib import Path
from typing import Dict

from datapackage import Resource
from glom import glom
import pandas as pd
from pandas._libs.parsers import STR_NA_VALUES

from sark._types import _path_t
from sark.helpers import consume, import_from

# TODO: compressed files
_source_ts = ["csv", "xls", "xlsx"]  # "sqlite"
_pd_types = {
    "boolean": "bool",
    "date": "datetime64",
    "time": "datetime64",
    "datetime": "datetime64",
    "year": "datetime64",
    "yearmonth": "datetime64",
    "integer": "Int64",
    "number": "float",
    "string": "string",
}


def _source_type(source: _path_t) -> str:
    """From a file path, deduce the file type from the extension

    Note: the extension is checked against the list of supported file types

    """
    # FIXME: use file magic
    source_t = Path(source).suffix.strip(".").lower()
    if source_t not in _source_ts:
        raise ValueError(f"unsupported source: {source_t}")
    return source_t


def _schema(resource: Resource, type_map: Dict[str, str]) -> Dict[str, str]:
    """Parse a Resource schema and return types mapped to each column.

    Parameters
    ----------
    resource
        A resource descriptor
    type_map : Dict[str, str]
        A dictionary that maps datapackage type names to pandas types.

    Returns
    -------
    Dict[str, str]
        Dictionary with column names as key, and types as values

    """
    return dict(
        glom(
            resource,  # target
            (  # spec
                "schema.fields",  # Resource & Schema properties
                [  # fields inside a list
                    (
                        "descriptor",  # Field property
                        # string names -> string dtypes understood by pandas
                        lambda t: (t["name"], type_map[t["type"]]),
                    )
                ],
            ),
        )
    )


def to_df(resource: Resource, noexcept: bool = False) -> pd.DataFrame:
    """Reads a data package resource as a `pandas.DataFrame`

    FIXME: only considers 'name' and 'type' in the schema, other options like
    'format', 'missingValues', etc are ignored.

    Parameters
    ----------
    resource : `datapackage.Resource`
        A data package resource object
    noexcept : bool (default: False)
        Whether to suppress an exception

    Returns
    -------
    pandas.DataFrame
        NOTE: when `noexcept` is `True`, and there's an exception, an empty
        dataframe is returned

    Raises
    ------
    ValueError
        If the resource is not local
        If the source type the resource is pointing to isn't supported

    """
    if not resource.local:  # pragma: no cover, not implemented
        if noexcept:
            return pd.DataFrame()
        else:
            raise ValueError(f"{resource.source}: not a local resource")

    pd_readers = {
        "csv": "read_csv",
        "xls": "read_excel",
        "xlsx": "read_excel",
        # "sqlite": "read_sql",
    }
    try:
        reader = import_from("pandas", pd_readers[_source_type(resource.source)])
    except ValueError:
        if noexcept:
            return pd.DataFrame()
        else:
            raise

    # parse dates
    schema = _schema(resource, _pd_types)
    date_cols = [col for col, col_t in schema.items() if "datetime64" in col_t]
    consume(map(schema.pop, date_cols))

    # missing values, NOTE: pandas accepts a list of "additional" tokens to be
    # treated as missing values.
    na_values = glom(resource, ("descriptor.schema.missingValues", set)) - STR_NA_VALUES
    # FIXME: check if empty set is the same as None

    # FIXME: how to handle constraints? e.g. 'required', 'unique', 'enum', etc
    # see: https://specs.frictionlessdata.io/table-schema/#constraints

    # set 'primaryKey' as index_col, a list is interpreted as a MultiIndex
    index_col = glom(resource, ("descriptor.schema.primaryKey"), default=False)

    return reader(
        resource.source,
        dtype=schema,
        na_values=na_values,
        index_col=index_col,
        parse_dates=date_cols,
    )
