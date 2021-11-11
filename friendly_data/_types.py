from pathlib import Path
from typing import Dict, TypeVar, Union

import pandas as pd

_path_t = Union[str, Path]  # file path type
_license_t = Dict[str, str]
_dfseries_t = TypeVar("_dfseries_t", pd.DataFrame, pd.Series)
