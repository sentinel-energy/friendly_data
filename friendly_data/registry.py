"""Configurable Friendly data schema registry

Module to wrap around the default :module:`friendly_data_registry` to add
configurability.  A custom registry configuration can be specified by using the
:func:`config_ctx` context manager.  The :class:`RegistrySchema` validates the
registry config before customising the default registry.

"""

from contextlib import contextmanager
from logging import getLogger
from typing import cast, Dict, List

from glom import glom, Iter, Optional as optmatch
from glom import MatchError, TypeMatchError

from friendly_data._types import _path_t
from friendly_data.helpers import match
from friendly_data.io import dwim_file
import friendly_data_registry as _registry

logger = getLogger(__name__)
_custom: Dict[str, List[Dict]] = {}


class RegistrySchema(_registry.schschemaema):
    """Instantiate with the "registry" section of the config file to validate

    The registry section looks like this:

    .. code-block:: yaml

      registry:
        idxcols:
          - name: enduse
            type: string
            constraints:
              enum:
                - ...
        cols:
          - name: cost
            type: number
            constraints:
              minimum: 0


    """

    # overwrite cls._schema from the base class
    _schema = {
        optmatch(col_t): [_registry.schschemaema._schema]
        for col_t in ("idxcols", "cols")
    }

    def __init__(self, registry_config: Dict[str, List[Dict]]):
        """Initialise to verify config

        Parameters
        ----------
        registry_config : Dict[str, List[Dict]]

            The "registry" section from the config file, or any consolidated
            schema registry.

        Raises
        ------
        TypeMatchError
            When the registry config has a type mismatch
        MatchError
            Other mismatches like, an incorrectly named key


        """
        try:
            super().__init__(registry_config)
        except TypeMatchError as err:
            e, f = err.args[1:]
            logger.error(f"type mismatch: expected {e}, found {f}")
            raise err from None
        except MatchError as err:
            logger.error(f"{err.args[1]}: bad key in schema")
            raise err from None


@contextmanager
def config_ctx(
    *,
    confdict: Dict[str, List[Dict]] = {},
    conffile: _path_t = "",
    idxcols: List[Dict] = [],
    cols: List[Dict] = [],
):
    """Context manager to temporarily override the default registry

    Note that the parameters are allowed only as a keyword argument, and
    multiple parameters are not allowed at the same time.  They are checked in
    the same order as shown here, and on finding one, following parameters are
    ignored.

    The registry config is also validated.  If validation fails, an error
    message is logged, and the default registry remains unaltered.

    Parameters
    ----------
    confdict : Dict[str, List[Dict]]
        Registry config in dictionary form

    conffile : Union[str, Path]
        Path to a config file with a custom registry section

    idxcols : List[Dict]
        List of custom index columns

    cols : List[Dict]
        List of custom value columns

    Returns
    -------
    Generator[Dict[str, List[Dict]]]
        The custom registry config

    Example
    -------
    ::

      with config_ctx(conffile="config.yaml") as _:
          print(get("mycol", "cols"))
          print(getall())

    """
    global _custom
    save = {col_t: _custom.get(col_t, []) for col_t in ("idxcols", "cols")}
    custom = {}
    if confdict:
        custom = confdict
    elif conffile:
        custom = cast(Dict, dwim_file(conffile)).get("registry", {})
    elif idxcols:
        custom = {"idxcols": idxcols}
    elif cols:
        custom = {"cols": cols}
    try:
        _custom.update(RegistrySchema(custom))
    except MatchError as err:
        logger.error(f"ignoring bad custom registry: {err}")
        yield _custom
    else:
        yield _custom
    finally:
        _custom.update(save)


def get(col: str, col_t: str) -> Dict:
    global _custom
    reg = _registry.get(col, col_t)
    custom = glom(
        _custom,
        (col_t, Iter().filter(match({"name": col, str: object})).first()),
        default={},
    )
    if custom:
        reg.update(custom)  # override default registry
    return reg


def getall(with_file=False) -> Dict[str, List[Dict]]:
    global _custom
    reg = _registry.getall(with_file)
    for col_t, _cols in _custom.items():
        for _col in _cols:
            newcol = True
            for col in reg[col_t]:
                if col["name"] == _col["name"]:
                    col.update(_col)
                    newcol = False
                    break
            if newcol:
                reg[col_t].append(_col)
    return reg


doc_tmpl = """Wraps around the getters in :func:`friendly_data_registry.{getter}`.

    If a custom registry config has been specified, columns from the config are
    also considered.  A custom registry config can be set using the
    :func:`config_ctx` context manager.

    """
get.__doc__ = doc_tmpl.format(getter="get")
getall.__doc__ = doc_tmpl.format(getter="getall")
