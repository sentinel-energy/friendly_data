"""Functions useful to interact with a data package.

"""
# PS: the coincidential module name is intentional ;)

from itertools import chain
import json
from pathlib import Path
from typing import cast, Dict, Iterable, List, Optional, Tuple, TypeVar
from warnings import warn
from zipfile import ZipFile

from frictionless import Layout, Package, Resource
from glom import Assign, glom, Invoke, Iter, Spec, T
import pandas as pd
from pkg_resources import resource_filename

from sark.io import dwim_file, path_not_in, posixpathstr, relpaths
from sark.helpers import match, select, is_windows
from sark._types import _path_t
import sark_registry as registry


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


def _resource(spec: Dict, basepath: _path_t = "") -> Resource:
    """Create a Resource object based on the dictionary

    Parameters
    ----------
    spec : Dict
        Dictionary with the structure::

          {"path": "relpath/resource.csv", "skip": <nrows>}

        "skip" is optional.

    basepath : Union[str, Path]
        Base path for resource object

    Returns
    -------
    Resource

    """
    assert "path" in spec, f"Incomplete resource spec: {spec}"
    opts = {}
    if "skip" in spec:
        # FIXME: `offset_rows` doesn't seem to work, so workaround with
        # `skip_rows` (`frictionless` expects a 1-indexed array).  `pandas` on
        # the other hand uses a 0-indexed list, which has to be accounted for
        # in `to_df`
        opts["layout"] = Layout(skip_rows=[i + 1 for i in range(spec["skip"])])
    res = Resource(path=str(spec["path"]), basepath=str(basepath), **opts)
    return res


_res_t = TypeVar("_res_t", str, Path, Dict)


def create_pkg(meta: Dict, fpaths: Iterable[_res_t], basepath: _path_t = ""):
    """Create a datapackage from metadata and resources.

    If `resources` point to files that exist, their schema are inferred and
    added to the package.  If `basepath` is a non empty string, it is treated
    as the parent directory, and all resource file paths are checked relative
    to it.

    Parameters
    ----------
    meta : Dict
        A dictionary with package metadata.

    fpaths : Iterable[Union[str, Path]]
        An iterator over different resources.  Resources are paths to files,
        relative to `basepath`.

    basepath : str (default: empty string)
        Directory where the package files are located

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
    pkg = Package(meta, basepath=str(basepath))

    # TODO: should we handle adding resources by descriptor? Package.add_resource(..)
    def keep(res: _path_t) -> bool:
        if Path(res) in existing:
            return False
        full_path = Path(basepath) / res
        if not full_path.exists():
            warn(f"{full_path}: skipped, doesn't exist", RuntimeWarning)
            return False
        return True

    for res in fpaths:
        spec = {"path": res} if isinstance(res, (str, Path)) else res
        if not keep(spec["path"]):
            continue
        _res = _resource(spec, basepath=basepath)
        _res.infer()
        pkg.add_resource(_res)

    return _ensure_posix(pkg)


def read_pkg(pkg_path: _path_t, extract_dir: Optional[_path_t] = None):
    """Read a  datapackage

    If `pkg_path` points to a `datapackage.json` file, read it as is.  If it
    points to a zip archive.  The archive is first extracted before opening it.
    If `extract_dir` is not provided, the current directory of the zip archive
    is used.

    Parameters
    ----------
    pkg_path : Union[str, Path]
        Path to the `datapackage.json` file, or a zip archive

    extract_dir : Union[str, Path]
        Path to which the zip archive is extracted

    Returns
    -------
    Package

    Raises
    ------
    ValueError
        When an unsupported file format (not JSON or ZIP) is provided

    """
    pkg_path = Path(pkg_path)
    if pkg_path.suffix == ".json":
        with open(pkg_path) as pkg_json:
            basepath = f"{Path(pkg_path).parent}"
            pkg = Package(json.load(pkg_json), basepath=basepath)
    elif pkg_path.suffix == ".zip":
        if extract_dir is None:
            extract_dir = pkg_path.parent
        else:
            extract_dir = Path(extract_dir)
        with ZipFile(pkg_path) as pkg_zip:
            pkg_zip.extractall(path=extract_dir)
            with open(extract_dir / "datapackage.json") as pkg_json:
                pkg = Package(json.load(pkg_json), basepath=f"{extract_dir}")
    elif pkg_path.is_dir():
        with open(pkg_path / "datapackage.json") as pkg_json:
            pkg = Package(json.load(pkg_json), basepath=str(pkg_path))
    else:
        raise ValueError(f"{pkg_path}: expecting a JSON or ZIP file")
    return _ensure_posix(pkg)


def update_pkg(pkg: Package, resource: str, schema_update: Dict, fields: bool = True):
    """Update package resource schema

    Parameters
    ----------
    pkg : Package
        Package object

    resource : str
        Resource name FIXME: cannot handle duplicate names in subdirectories

    schema_update : Dict
        Updated fields in the schema, if `field` is `False`, can be used to
        update `missingValues`, or `primaryKey`.  When updating the schema, it
        looks like this ('foo'/'bar' are names of the fields being updated)::

          {
              "foo": {
                  "name": "foo",
                  "type": "datetime",
                  "format": "default"
              },
              "bar": {
                  "name": "bar",
                  "type": "integer",
                  "format": "default"
              }
          }

    fields : bool (default: True)
        If the schema update is a field, or not

    Returns
    -------
    bool
        Return the `Package.valid` flag; `True` if the update was valid.

    """
    assert "resources" in pkg, "Package should have at least one resource"
    res, *_ = [res for res in pkg["resources"] if res["name"] == resource]
    if fields:
        for field in res["schema"]["fields"]:
            if field["name"] in schema_update:
                field.update(schema_update[field["name"]])
    else:
        # FIXME: do the following checks w/o asserts
        assert "fields" not in schema_update, "cannot add fields to schema"
        # prevents from adding additional keys
        assert set(schema_update) - {"primaryKey", "missingValues"} == set()
        res["schema"].update(schema_update)
    return pkg.metadata_valid


def read_pkg_index(fpath: _path_t) -> pd.DataFrame:
    """Read the index of files incuded in the datapackage

    Parameters
    ----------
    fpath : Union[str, Path]
        Index file path or a stream object

    Returns
    -------
    pd.DataFrame
        A dataframe with the columns: 'file', 'name', and 'idxcols'; 'idxcols'
        is a tuple.

    Raises
    ------
    ValueError
        If the file type is correct (YAML/JSON), but does not return a list
    RuntimeError
        If the file has an unknown extension (raised by :func:`sark.io.dwim_file`)

    """
    idx = dwim_file(Path(fpath))
    if not isinstance(idx, list):
        raise ValueError(f"{fpath}: bad index file")

    # # convert list (idxcols) into tuples, easier to query in DataFrame
    # glom(idx, [Assign("idxcols", Spec((T["idxcols"], tuple)))])
    return pd.DataFrame(idx)


def index_levels(_file: _path_t, idxcols: Iterable[str]) -> Tuple[_path_t, Dict]:
    """Read a dataset and determine the index levels

    Parameters
    ----------
    _file : Union[str, Path]
        Path to the dataset

    idxcols : Iterable[str]
        List of columns in the dataset that constitute the index

    Returns
    -------
    Tuple[str, Dict]

        Tuple of path to the dataset, and the schema of each column as a dictionary.
        If `idxcols` was ["foo", "bar"], the dictionary might look like::

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
    coldict = {col: registry.get(col, "idxcols") for col in idxcols}
    # select columns with an enum constraint where the enum values are empty
    select_cols = match({"constraints": {"enum": []}, str: str})
    cols = glom(coldict.values(), Iter().filter(select_cols).map("name").all())
    idx = pd.read_csv(_file, index_col=cols).index
    if isinstance(idx, pd.MultiIndex):
        levels = {col: list(lvls) for col, lvls in zip(idx.names, idx.levels)}
    else:
        levels = {idx.names[0]: list(idx.unique())}
    enum_vals = Spec(Invoke(levels.__getitem__).specs("name"))
    glom(
        coldict.values(),
        Iter().filter(select_cols).map(Assign("constraints.enum", enum_vals)).all(),
    )
    return _file, coldict


def pkg_from_index(meta: Dict, fpath: _path_t) -> Tuple[Path, Package, pd.DataFrame]:
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
    Tuple[Path, Package, pandas.DataFrame]
        The package directory, the `Package` object, and the index dataframe.

    Examples
    --------

    YAML::

        >>> yaml_f = '''
        ... - path: file1
        ...   name: dst1
        ...   idxcols: [cola, colb]
        ... - path: file2
        ...   name: dst2
        ...   idxcols: [colx, coly, colz]
        ... - path: file3
        ...   name: dst3
        ...   idxcols: [col]
        ... '''

    JSON::

        >>> json_f = '''
        ... [
        ...     {
        ...         "path": "file1",
        ...         "name": "dst1",
        ...         "idxcols": [
        ...             "cola",
        ...             "colb"
        ...         ]
        ...     },
        ...     {
        ...         "path": "file2",
        ...         "name": "dst2",
        ...         "idxcols": [
        ...             "colx",
        ...             "coly",
        ...             "colz"
        ...         ]
        ...     },
        ...     {
        ...         "path": "file3",
        ...         "name": "dst3",
        ...         "idxcols": [
        ...             "col"
        ...         ]
        ...     }
        ... ]
        ... '''

    Index as read from the example above::

        >>> idx = read_pkg_index("testing/files/indices/index.json")
        >>> idx
             path  name             idxcols
        0   file1  dst1        [cola, colb]
        1   file2  dst2  [colx, coly, colz]
        2   file3  dst3               [col]

    """
    pkg_dir = Path(fpath).parent
    idx = read_pkg_index(fpath)
    if "skip" in idx.columns:
        resources = idx[["path", "skip"]].to_dict("records")
    else:
        resources = idx["path"].to_list()
    pkg = create_pkg(meta, resources, basepath=f"{pkg_dir}")
    for entry in idx.to_records():
        resource_name = Path(entry.path).stem
        _, update = index_levels(pkg_dir / entry.path, entry.idxcols)
        update_pkg(pkg, resource_name, update)
        update_pkg(pkg, resource_name, {"primaryKey": entry.idxcols}, fields=False)
        # set of value columns
        cols = (
            glom(
                pkg,
                (
                    "resources",
                    Iter()
                    .filter(select("path", equal_to=entry.path))
                    .map("schema.fields")
                    .flatten()
                    .map("name")
                    .all(),
                    set,
                ),
            )
            - set(entry.idxcols)
        )
        update_pkg(pkg, resource_name, {col: registry.get(col, "cols") for col in cols})
    return pkg_dir, pkg, idx


def pkg_glossary(pkg: Package, idx: pd.DataFrame) -> pd.DataFrame:
    """Generate glossary from the package and the package index.

    Parameters
    ----------
    pkg : Package

    idx : pd.DataFrame
        The index dataframe

    Returns
    -------
    pd.DataFrame
        The glossary as dataframe

    """
    _levels = lambda row: glom(
        pkg,
        (
            "resources",
            Iter()
            .filter(select("path", equal_to=row["path"]))
            .map("schema.fields")
            .flatten()
            .filter(
                match({"name": row["idxcols"], "constraints": {"enum": list}, str: str})
            )
            .map("constraints.enum")
            .flatten()
            .all(),
        ),
    )
    glossary = idx.explode("idxcols").reset_index(drop=True)
    return glossary.assign(values=glossary.apply(_levels, axis=1))


def pkg_from_files(meta: Dict, fpath: _path_t, fpaths: Iterable[_path_t]):
    """Create a package from an index file and other files

    Parameters
    ----------
    meta : Dict
        A dictionary with package metadata.

    fpath : Union[str, Path]
        Path to the package directory or index file.  Note the index file has
        to be at the top level directory of the datapackage.  See
        :func:`sark.dpkg.read_pkg_index`

    fpaths : List[Union[str, Path]]
        A list of paths to datasets/resources not in the index.  If any of the
        paths point to a dataset already present in the index, it is ignored.

    Returns
    -------
    Tuple[Path, Package, pd.DataFrame]
        A datapackage with inferred schema for the resources/datasets present
        in the index; all other resources are added with a basic inferred
        schema.

    """
    fpath = Path(fpath)
    idxpath = idxpath_from_pkgpath(fpath) if fpath.is_dir() else fpath
    if idxpath:
        pkgdir, pkg, idx = pkg_from_index(meta, idxpath)
        # convert to full path
        idx_fpath = cast(Iterable[_path_t], idx["path"].apply(pkgdir.__truediv__))
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
    RuntimeWarning
        - Warns if no index file is not found
        - Warns if multiple index files are found

    """
    pkgpath = Path(pkgpath)
    idxpath = [
        p
        for p in sorted(pkgpath.glob("index.*"))
        if p.suffix in (".yaml", ".yml", ".json")
    ]
    if not idxpath:
        warn(f"{pkgpath}: no index file found", RuntimeWarning)
        return ""
    elif len(idxpath) > 1:
        warn(
            f"multiple indices: {','.join(map(str, idxpath))}, using {idxpath[0]}",
            RuntimeWarning,
        )
    return idxpath[0]


def write_pkg(
    pkg: Package,
    pkgdir: _path_t,
    *,
    idx: Optional[pd.DataFrame] = None,
    glossary: Optional[pd.DataFrame] = None,
) -> List[Path]:
    """Write a data package to path

    Parameters
    ----------
    pkg: Package
        Package object

    pkgdir: Union[str, Path]
        Path to write to

    idx : pandas.DataFrame (optional)
        Package index written to `pkgdir/index.json`

    glossary : pandas.DataFrame (optional)
        Package glossary written to `pkgdir/glossary.json`

    Returns
    -------
    List[Path]
        List of files written to disk

    """
    pkgdir = Path(pkgdir)
    files = [pkgdir / "datapackage.json"]
    dwim_file(files[-1], pkg)

    if isinstance(idx, pd.DataFrame):
        files.append(pkgdir / "index.json")
        dwim_file(files[-1], idx.to_dict(orient="records"))

    if isinstance(glossary, pd.DataFrame):
        files.append(pkgdir / "glossary.json")
        dwim_file(files[-1], glossary.to_dict(orient="records"))

    # TODO: support saving to archives (zip, tar, etc)
    return files
