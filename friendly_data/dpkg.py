"""Functions useful to interact with a data package.

"""
# PS: the coincidential module name is intentional ;)

from logging import getLogger
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, TypeVar, Union, overload
from zipfile import ZipFile

from frictionless import Detector, Layout, Package, Resource
from glom import Assign, Coalesce, glom, Invoke, Iter, Spec, SKIP
from glom import Match, MatchError, Optional as optmatch, Or
import pandas as pd

from friendly_data.io import dwim_file, path_not_in, posixpathstr, relpaths
from friendly_data.helpers import match, noop_map, is_windows
from friendly_data.metatools import resolve_licenses
from friendly_data._types import _path_t, _dfseries_t
import friendly_data.registry as registry

logger = getLogger(__name__)


def _ensure_posix(pkg):
    """Ensure resource paths in the package are POSIX compliant

    FIXME: The :class:`datapackage.Package` implementation does not ensure
    paths are POSIX paths on Windows, correct them after the fact.  This is a
    temporary solution; see:
    https://github.com/frictionlessdata/datapackage-py/issues/279

    """
    if is_windows():
        to_posix = Spec(Invoke(posixpathstr).specs("path"))
        glom(pkg, ("resources", Iter().map(Assign("path", to_posix)).all()))
    return pkg


def fullpath(resource: Resource) -> Path:
    """Get full path of a resource

    Parameters
    ----------
    resource : Resource
        Resource object/dictionary

    Returns
    -------
    Path
        Full path to the resource

    """
    return Path(resource.basepath) / resource["path"]


def resource_(spec: Dict, basepath: _path_t = "", infer=True) -> Resource:
    """Create a Resource object based on the dictionary

    Parameters
    ----------
    spec : Dict
        Dictionary with the structure::

          {"path": "relpath/resource.csv", "skip": <nrows>}

        "skip" is optional.

    basepath : Union[str, Path]
        Base path for resource object

    infer : bool (default: True)
        Whether to infer resource schema

    Returns
    -------
    Resource

    """
    if isinstance(spec, Resource):
        return spec
    assert "path" in spec, f"Incomplete resource spec: {spec}"
    opts = {}
    layout_opts: Dict[str, Any] = {"header": True}
    if spec.get("skip"):
        # FIXME: `offset_rows` doesn't seem to work, so workaround with
        # `skip_rows` (`frictionless` expects a 1-indexed array).  `pandas` on
        # the other hand uses a 0-indexed list, which has to be accounted for
        # in `to_df`
        layout_opts["skip_rows"] = [i + 1 for i in range(spec["skip"])]
    opts["layout"] = Layout(**layout_opts)
    if spec.get("schema"):
        opts["detector"] = Detector(schema_patch=spec["schema"])
    res = Resource(path=str(spec["path"]), basepath=str(basepath), **opts)
    if infer:
        res.infer()
    empty = glom(res, ("schema.fields", Iter("type").filter(match("any")).all(), len))
    if empty:
        logger.warning(f"{res['path']} has empty columns")
    return res


_res_t = TypeVar("_res_t", str, Path, Dict)


def create_pkg(
    meta: Dict, fpaths: Iterable[_res_t], basepath: _path_t = "", infer=True
):
    """Create a datapackage from metadata and resources.

    If ``resources`` point to files that exist, their schema are inferred and
    added to the package.  If ``basepath`` is a non empty string, it is treated
    as the parent directory, and all resource file paths are checked relative
    to it.

    Parameters
    ----------
    meta : Dict
        A dictionary with package metadata.

    fpaths : Iterable[Union[str, Path, Dict]]
        An iterator over different resources.  Resources are paths to files,
        relative to ``basepath``.

    basepath : str (default: empty string)
        Directory where the package files are located

    infer : bool (default: True)
        Whether to infer resource schema

    Returns
    -------
    Package
        A datapackage with inferred schema for all the package resources

    """
    # for an interesting discussion on type hints with unions, see:
    # https://stackoverflow.com/q/60235477/289784

    # TODO: filter out and handle non-tabular (custom) data
    existing = glom(meta.get("resources", []), Iter("path").map(Path).all())
    basepath = basepath if basepath else getattr(meta, "basepath", basepath)
    pkg = Package(resolve_licenses(meta), basepath=str(basepath))

    def keep(res: _path_t) -> bool:
        if Path(res) in existing:
            return False
        full_path = Path(basepath) / res
        if not full_path.exists():
            logger.warning(f"{full_path}: skipped, doesn't exist")
            return False
        return True

    for res in fpaths:
        spec = res if isinstance(res, dict) else {"path": res}
        if not keep(spec["path"]):
            continue
        # NOTE: noop when Resource
        _res = resource_(spec, basepath=basepath, infer=infer)
        pkg.add_resource(_res)

    return _ensure_posix(pkg)


def read_pkg(pkg_path: _path_t, extract_dir: Optional[_path_t] = None):
    """Read a  datapackage

    If ``pkg_path`` points to a ``datapackage.json`` file, read it as is.  If it
    points to a zip archive.  The archive is first extracted before opening it.
    If ``extract_dir`` is not provided, the current directory of the zip archive
    is used.  If it is a directory, look for a ``datapackage.json`` inside.

    Parameters
    ----------
    pkg_path : Union[str, Path]
        Path to the ``datapackage.json`` file, or a zip archive

    extract_dir : Union[str, Path]
        Path to which the zip archive is extracted

    Returns
    -------
    Package

    Raises
    ------
    ValueError
        When an unsupported format (not a directory, JSON, or ZIP) is provided
    FileNotFoundError
        When a datapackage.json file cannot be found

    """
    pkg_path = Path(pkg_path)
    if pkg_path.suffix == ".json":
        pkg_json = pkg_path
        basepath = Path(pkg_path).parent
    elif pkg_path.suffix == ".zip":
        if extract_dir is None:
            extract_dir = pkg_path.parent
        else:
            extract_dir = Path(extract_dir)
        with ZipFile(pkg_path) as pkg_zip:
            pkg_zip.extractall(path=extract_dir)
            pkg_json = extract_dir / "datapackage.json"
            basepath = extract_dir
    elif pkg_path.is_dir():
        pkg_json = pkg_path / "datapackage.json"
        basepath = pkg_path

    else:
        raise ValueError(f"{pkg_path}: not a directory, JSON, or ZIP file")

    if pkg_json.exists():
        pkg = Package(dwim_file(pkg_json), basepath=f"{basepath}")
    else:
        msg = f"{pkg_json}: not found"
        logger.error(msg)
        raise FileNotFoundError(msg)

    return _ensure_posix(pkg)


class pkgindex(List[Dict]):
    """Data package index (a subclass of ``list``)

    It is a list of dictionaries, where each dictionary is the respective
    record for a file.  A record may have the following keys:

    - "path": path to the file,
    - "idxcols": list of column names that are to be included in the dataset
      index (optional),
    - "name": dataset name (optional),
    - "skip": lines to skip when reading the dataset (optional, CSV only),
    - "alias": a mapping of column name aliases (optional),
    - "sheet": sheet name or position (0-indexed) to use as dataset (optional,
      Excel only)

    While iterating over an index, always use :meth:`~pkgindex.records` to
    ensure all necessary keys are present.

    """

    # set of keys that are accepted in a data package
    _key_map = {
        "path": str,
        "idxcols": [str],
        "name": str,
        "description": str,
        "skip": int,
        "alias": {str: str},
        "sheet": Or(int, str),
        "iamc": str,  # FIXME: Regex("^[0-9a-zA-Z_ |-{}]+$"),
        "agg": {str: [{"values": [str], "variable": str}]},
    }
    _optional = [
        "idxcols",
        "name",
        "description",
        "skip",
        "alias",
        "sheet",
        "iamc",
        "agg",
    ]

    @classmethod
    def from_file(cls, fpath: _path_t) -> "pkgindex":
        """Read the index of files included in the data package

        Parameters
        ----------
        fpath : Union[str, Path]
            Index file path or a stream object

        Returns
        -------
        List[Dict]

        Raises
        ------
        ValueError
            If the file type is correct (YAML/JSON), but does not return a list
        RuntimeError
            If the file has an unknown extension (raised by :func:`friendly_data.io.dwim_file`)
        MatchError
            If the file contains any unknown keys

        """  # noqa: E501
        idx = dwim_file(Path(fpath))
        if not isinstance(idx, list):
            raise ValueError(f"{fpath}: bad index file")
        return cls(cls._validate(idx))

    @classmethod
    def _validate(cls, idx: List[Dict]) -> List[Dict]:
        record_match = Match(
            {
                optmatch(k) if k in cls._optional else k: v
                for k, v in cls._key_map.items()
            }
        )
        try:
            return glom(idx, Iter(record_match).all())
        except MatchError as err:
            logger.error(f"{err.args[1]}: bad key in index file")
            raise err from None

    @overload
    @classmethod
    def _validate_keys(cls, keys: str) -> str:
        ...  # pragma: no cover, overload

    @overload
    @classmethod
    def _validate_keys(cls, keys: List[str]) -> List[str]:
        ...  # pragma: no cover, overload

    @classmethod
    def _validate_keys(cls, keys):
        if isinstance(keys, str):
            return glom(keys, Match(Or(*cls._key_map.keys())))
        else:
            return glom(keys, [Match(Or(*cls._key_map.keys()))])

    def records(self, keys: List[str]) -> Iterable[Dict]:
        """Return an iterable of index records.

        Each record is guaranteed to have all the requested keys.  If a value
        wasn't specified in the index file, it is set to ``None``.

        Parameters
        ----------
        keys : List[str]
            List of keys that are requested in each record.

        Returns
        -------
        Iterable[Dict]

        Raises
        ------
        glom.MatchError
            If ``keys`` has an unsupported value

        """
        spec = {k: Coalesce(k, default=None) for k in self._validate_keys(keys)}
        return glom(self, Iter().map(spec).all())

    def get(self, key: str) -> List:
        """Get the value of ``key`` from all records as a list.

        If ``key`` is absent, the corresponding value is set to ``None``.

        Parameters
        ----------
        key : str
            Key to retrieve

        Returns
        -------
        List
            List of records with values corresponding to ``key``.

        """
        return glom(self, [Coalesce(self._validate_keys(key), default=None)])


# FIXME: fix col_t annotations when we drop Python 3.7
def get_aliased_cols(cols: Iterable[str], col_t: str, alias: Dict[str, str]) -> Dict:
    """Get aliased columns from the registry

    Parameters
    ----------
    cols : Iterable[str]
        List of columns to retrieve

    col_t : Literal["cols", "idxcols"]
        A literal string specifying the kind of column; one of: "cols", or "idxcols"

    alias : Dict[str, str]
        Dictionary of aliases; key is the column name in the dataset, and the
        value is the column name in the registry that it is equivalent to.

    Returns
    -------
    Dict
        Schema for each column, the column name is the key, and the schema is
        the value; see the doctring of :func:`index_levels` for more.

    """
    alias = noop_map(alias if isinstance(alias, dict) else {})
    coldict = {col: {**registry.get(alias[col], col_t), "name": col} for col in cols}
    for col in cols:
        if col in alias:
            coldict[col]["alias"] = alias[col]
    return coldict


def index_levels(
    file_or_df: Union[_path_t, _dfseries_t],
    idxcols: Iterable[str],
    alias: Dict[str, str] = {},
) -> Tuple[_dfseries_t, Dict]:
    """Read a dataset and determine the index levels

    Parameters
    ----------
    file_or_df : Union[str, Path, pd.DataFrame, pd.Series]
        A dataframe, or the path to a CSV file

    idxcols : Iterable[str]
        List of columns in the dataset that constitute the index

    alias : Dict[str, str]
        Column aliases: {my_alias: col_in_registry}

    Returns
    -------
    Tuple[Union[pd.DataFrame, pd.Series], Dict]

        Tuple of the dataset, and the schema of each index column as a
        dictionary.  If ``idxcols`` was ["foo", "bar"], the dictionary might look
        like::

          {
              "foo": {
                  "name": "foo",
                  "type": "datetime",
                  "format": "default"
              },
              "bar": {
                  "name": "bar",
                  "type": "string",
                  "constraints": {
                      "enum": ["a", "b"]
                  }
              }
          }

        Note that the index columns that have categorical values, are filled in
        by reading the dataset and determining the full set of values.

    """
    coldict = get_aliased_cols(idxcols, "idxcols", alias)
    # 4 cases:
    # 1) in registry, and enum constraints set:
    #    {"constraints": {"enum": And(list, len)}, str: str}
    # 2) in registry, but enum constraints unset: `enum_partial` below
    # 3) in registry, but enum not applicable (e.g. datetime): nothing to match
    # 4) not in registry; this has a few possibilities, the type could be
    #    string, integer, or boolean, but the metadata isn't readily available.
    #    It could be deduced from the dataframe, but that is a bit messy as it
    #    requires reverse lookup of `converters._pd_types`.  There should be a
    #    cost to not using the registry, and this is it - it will not be enum
    #    constrained:
    #    And(
    #        {"type": Or("string", "integer", "boolean"), str: str},
    #        lambda i: "constraints" not in i,
    #    )
    #
    # select columns with an enum constraint where the enum values are empty
    enum_partial = {"constraints": {"enum": []}, str: str}
    select_cols = match(enum_partial)
    cols = glom(coldict.values(), Iter().filter(select_cols).map("name").all())

    if isinstance(file_or_df, (pd.DataFrame, pd.Series)):
        if not cols:
            return file_or_df, coldict
        diff = list(set(idxcols) - set(cols))
        idx = file_or_df.index.droplevel(diff)
    else:
        file_or_df = pd.read_csv(file_or_df, index_col=cols)
        if not cols:
            return file_or_df, coldict
        idx = file_or_df.index
    if isinstance(idx, pd.MultiIndex):
        levels = {col: list(lvls) for col, lvls in zip(idx.names, idx.levels)}
    else:
        levels = {idx.names[0]: list(idx.unique())}
    enum_vals = Spec(Invoke(levels.__getitem__).specs("name"))
    glom(
        coldict.values(),
        Iter().filter(select_cols).map(Assign("constraints.enum", enum_vals)).all(),
    )
    return file_or_df, coldict


def set_idxcols(fpath: _path_t, basepath: _path_t = "") -> Resource:
    """Create a resource object for a file, with index columns set

    Parameters
    ----------
    fpath : Union[str, Path]
        Path to a dataset (resource), e.g. a CSV file, relative to `basepath`

    basepath : Union[str, Path], default: empty string (current directory)
        Path to directory to consider as the data package basepath

    Returns
    -------
    Resource
        Resource object with the index columns set according to the registry

    """
    res = resource_({"path": fpath}, basepath=basepath)
    all_idxcols = glom(registry.getall()["idxcols"], ["name"])
    idxcols = glom(
        res, ("schema.fields", Iter("name").filter(lambda i: i in all_idxcols).all())
    )
    return glom(res, Assign("schema.primaryKey", idxcols))


def res_from_entry(entry: Dict, pkg_dir: _path_t) -> Resource:
    """Create a resource from an index entry.

    Entry must have the keys: ``path``, ``idxcols``, ``alias``; so use
    :meth:`pkgindex.records` to iterate over the index.

    Parameters
    ----------
    entry : Dict
        Entry from an index file::

          {
            "path": "data.csv"
            "idxcols": ["col1", "col2"]
            "alias": {
              "col1": "col0"
            }
          }

    pkg_dir : Union[str, Path]
        Root directory of the package

    Returns
    -------
    Resource
        The resource object (subclass of ``dict``)

    """
    mandatory = ("path", "idxcols", "alias")
    if not all(map(lambda i: i in entry, mandatory)):
        msg = f"one of {mandatory} missing in {entry}"
        msg += "\nDid you call idx.records(keys) to iterate?"
        logger.exception(msg)
        raise ValueError(msg)
    try:
        _, idxcoldict = index_levels(
            Path(pkg_dir) / entry["path"], entry["idxcols"], entry["alias"]
        )
    except Exception as err:
        # FIXME: too broad; most likely this fails because of bad options
        # (e.g. index file entry), validate options and narrow scope of except
        logger.exception(f"error reading {entry}")
        raise err from None
    entry.update(schema={"fields": idxcoldict, "primaryKey": entry["idxcols"]})
    # FIXME: should we wrap this in a similar try: ... except: ...
    res = resource_(entry, basepath=f"{pkg_dir}", infer=True)
    # set of value columns
    cols = glom(res.schema.fields, (Iter("name"), set)) - set(entry["idxcols"])
    coldict = get_aliased_cols(cols, "cols", entry["alias"])
    glom(
        res.schema.fields,
        Iter()
        .filter(match({"name": Or(*coldict.keys()), str: object}))
        .map(lambda i: i.update(coldict[i["name"]]))
        .all(),
    )
    return res


def entry_from_res(res: Resource) -> Dict:
    """Create an index entry from a ``Resource`` object

    Parameters
    ----------
    res : Resource
        A resource object

    Returns
    -------
    Dict
        A dictionary that is an index entry

    """
    entry = glom(
        res,
        {
            "name": "name",
            "path": "path",
            "idxcols": Coalesce("schema.primaryKey", default=SKIP),
        },
    )
    alias = glom(
        res,
        (
            "schema.fields",
            Iter(match({"alias": str, str: object}))
            .map(lambda i: (i["name"], i["alias"]))
            .all(),
            dict,
        ),
    )
    if alias:
        entry["alias"] = alias
    return entry


def pkg_from_index(meta: Dict, fpath: _path_t) -> Tuple[Path, Package, pkgindex]:
    """Read an index file, and create a datapackage with the provided metadata.

    The index can be in either YAML, or JSON format.  It is a list of dataset
    files, names, and a list of columns in the dataset that are to be treated
    as index columns (see example below)

    Parameters
    ----------
    meta : Dict
        Package metadata dictionary

    fpath : Union[str, Path]
        Path to the index file.  Note the index file has to be at the top level
        directory of the datapackage.

    Returns
    -------
    Tuple[Path, Package, pkgindex]
        The package directory, the ``Package`` object, and the index.

    Examples
    --------

    YAML (JSON is also supported)::

        - path: file1
          name: dst1
          idxcols: [cola, colb]
        - path: file2
          name: dst2
          idxcols: [colx, coly, colz]
        - path: file3
          name: dst3
          idxcols: [col]

    """
    pkg_dir = Path(fpath).parent
    idx = pkgindex.from_file(fpath)
    resources = [
        res_from_entry(entry, pkg_dir)
        for entry in idx.records(["path", "idxcols", "skip", "alias"])
    ]  # NOTE: res_from_entry requires: "path", "idxcols", "alias"
    pkg = create_pkg(meta, resources, basepath=f"{pkg_dir}")
    return pkg_dir, pkg, idx


def pkg_from_files(
    meta: Dict, fpath: _path_t, fpaths: Iterable[_path_t]
) -> Tuple[Path, Package, Union[pkgindex, None]]:
    """Create a package from an index file and other files

    Parameters
    ----------
    meta : Dict
        A dictionary with package metadata.

    fpath : Union[str, Path]
        Path to the package directory or index file.  Note the index file has
        to be at the top level directory of the datapackage.  See
        :meth:`pkgindex.from_file`

    fpaths : List[Union[str, Path]]
        A list of paths to datasets/resources not in the index.  If any of the
        paths point to a dataset already present in the index, the index entry
        is respected.

    Returns
    -------
    Tuple[Path, Package, Union[pkgindex, None]]
        A datapackage with inferred schema for the resources/datasets present
        in the index; all other resources are added with a basic inferred
        schema.

    """
    fpath = Path(fpath)
    idxpath = idxpath_from_pkgpath(fpath) if fpath.is_dir() else fpath
    idx: Union[pkgindex, None]
    if idxpath:
        pkgdir, pkg, idx = pkg_from_index(meta, idxpath)
        # convert to full path
        idx_fpath = map(pkgdir.__truediv__, glom(idx, ["path"]))
        _fpaths = relpaths(pkgdir, filter(lambda p: path_not_in(idx_fpath, p), fpaths))
        pkg = create_pkg(pkg, _fpaths, basepath=pkgdir)
    else:
        pkgdir = fpath
        pkg = create_pkg(meta, relpaths(pkgdir, fpaths), basepath=pkgdir)
        idx = None
    return pkgdir, pkg, idx


def idxpath_from_pkgpath(pkgpath: _path_t) -> _path_t:
    """Return a valid index path given a package path

    Parameters
    ----------
    pkgpath : Union[str, Path]
        Path to package directory

    Returns
    -------
    Union[str, Path]
        - Returns a valid index path; if there are multiple matches, returns
          the lexicographically first match
        - If an index file is not found, returns an empty string

    Warns
    -----
        - Warns if no index file is found
        - Warns if multiple index files are found

    """
    pkgpath = Path(pkgpath)
    idxpath = [
        p
        for p in sorted(pkgpath.glob("index.*"))
        if p.suffix in (".yaml", ".yml", ".json")
    ]
    if not idxpath:
        logger.warning(f"{pkgpath}: no index file found")
        return ""
    elif len(idxpath) > 1:
        logger.warning(
            f"multiple indices: {','.join(map(str, idxpath))}, using {idxpath[0]}"
        )
    return idxpath[0]


def write_pkg(
    pkg: Union[Dict, Package],
    pkgdir: _path_t,
    *,
    idx: Union[pkgindex, List, None] = None,
) -> List[Path]:
    """Write a data package to path

    Parameters
    ----------
    pkg: Package
        Package object

    pkgdir: Union[str, Path]
        Path to write to

    idx : Union[pkgindex, List] (optional)
        Package index written to ``pkgdir/index.json``

    Returns
    -------
    List[Path]
        List of files written to disk

    """
    pkgdir = Path(pkgdir)
    files = [pkgdir / "datapackage.json"]
    dwim_file(files[-1], pkg)

    if isinstance(idx, (pkgindex, list)):
        files.append(pkgdir / "index.yaml")
        dwim_file(files[-1], list(idx))

    # TODO: support saving to archives (zip, tar, etc)
    return files
