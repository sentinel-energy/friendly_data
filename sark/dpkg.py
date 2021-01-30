"""Datapackage"""
# PS: the coincidential module name is intentional ;)

from itertools import chain
import json
from pathlib import Path
import sys
from typing import cast, Dict, Iterable, List, Optional, Tuple
from warnings import warn
from zipfile import ZipFile

from datapackage import Package
from glom import Assign, glom, Invoke, Iter, Spec, T
import pandas as pd
from pkg_resources import resource_filename

from sark.helpers import match, select
from sark.io import dwim_file, posixpathstr
from sark._types import _path_t


def create_pkg(meta: Dict, resources: Iterable[_path_t], base_path: _path_t = ""):
    """Create a datapackage from metadata and resources.

    If `resources` point to files that exist, their schema are inferred and
    added to the package.  If `base_path` is a non empty string, it is treated
    as the parent directory, and all resource file paths are checked relative
    to it.

    Parameters
    ----------
    meta : Dict
        A dictionary with package metadata.

    resources : Iterable[Union[str, Path]]
        An iterator over different resources.  Resources are paths to files,
        relative to `base_path`.

    base_path : str (default: empty string)
        Directory where the package files are located

    Returns
    -------
    Package
        A datapackage with inferred schema for all the package resources

    """
    # for an interesting discussion on type hints with unions, see:
    # https://stackoverflow.com/q/60235477/289784
    pkg = Package(meta, base_path=str(base_path))
    # TODO: filter out and handle non-tabular (custom) data
    existing = glom(meta.get("resources", []), Iter("path").map(Path).all())
    for res in resources:
        if Path(res) in existing:
            # list of resources may be paths, but descriptor will always be strings
            continue
        if isinstance(res, (str, Path)):
            full_path = Path(base_path) / res
            if not full_path.exists():
                warn(f"{full_path}: skipped, doesn't exist", RuntimeWarning)
                continue
            pkg.infer(f"{res}")
            print("create_pkg: adding ", pkg.descriptor["resources"][-1])
        else:  # pragma: no cover, adding with Dict (undocumented feature)
            pkg.add_resource(res)
    # FIXME: `Package` implementation does not ensure paths are POSIX paths on
    # Windows, so correct them after the fact.  This is a temporary solution;
    # see: https://github.com/frictionlessdata/datapackage-py/issues/279
    if sys.platform in ("win32", "cygwin"):
        to_posix = Spec(Invoke(posixpathstr).specs("path"))
        glom(
            pkg.descriptor,
            ("resources", Iter().map(Assign("path", to_posix)).all()),
        )
        pkg.commit()
    return pkg


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

    """
    pkg_path = Path(pkg_path)
    if pkg_path.suffix == ".json":
        with open(pkg_path) as pkg_json:
            base_path = f"{Path(pkg_path).parent}"
            return Package(json.load(pkg_json), base_path=base_path)
    elif pkg_path.suffix == ".zip":
        if extract_dir is None:
            extract_dir = pkg_path.parent
        else:
            extract_dir = Path(extract_dir)
        with ZipFile(pkg_path) as pkg_zip:
            pkg_zip.extractall(path=extract_dir)
            with open(extract_dir / "datapackage.json") as pkg_json:
                return Package(json.load(pkg_json), base_path=f"{extract_dir}")
    elif pkg_path.is_dir():
        with open(pkg_path / "datapackage.json") as pkg_json:
            return Package(json.load(pkg_json), base_path=str(pkg_path))
    else:
        raise ValueError(f"{pkg_path}: expecting a JSON or ZIP file")


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
    desc = pkg.descriptor
    assert "resources" in desc, "Package should have at least one resource"
    res, *_ = [res for res in desc["resources"] if res["name"] == resource]
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
    pkg.commit()
    return pkg.valid


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


# FIXME: can't use Literal until we drop 3.7
def registry(col: str, col_t: str) -> Dict:
    """Retrieve the column schema from column schema registry: `sark_registry`

    Parameters
    ----------
    col : str
        Column name to look for

    col_t : Literal["cols", "idxcols"]
        A literal string specifying the kind of column; one of: "cols", or "idxcols"

    Returns
    -------
    Dict
        Column schema; an empty dictionary is returned in case there are no matches

    Raises
    ------
    RuntimeError
        When more than one matches are found
    ValueError
        When the schema file in the registry is unsupported; not one of: JSON, or YAML

    """
    if col_t not in ("cols", "idxcols"):
        raise ValueError(f"{col_t}: unknown column type")

    curdir = Path(resource_filename("sark_registry", col_t))
    schema = list(
        chain.from_iterable(curdir.glob(f"{col}.{fmt}") for fmt in ("json", "yaml"))
    )
    if len(schema) == 0:
        warn(f"{col}: not in registry", RuntimeWarning)
        return {}  # no match, unregistered column
    if len(schema) > 1:  # pragma: no cover, bad registry
        raise RuntimeError(f"{schema}: multiple matches, duplicates in registry")
    return cast(Dict, dwim_file(curdir / schema[0]))


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
    coldict = {col: registry(col, "idxcols") for col in idxcols}
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
        ... - file: file1
        ...   name: dst1
        ...   idxcols: [cola, colb]
        ... - file: file2
        ...   name: dst2
        ...   idxcols: [colx, coly, colz]
        ... - file: file3
        ...   name: dst3
        ...   idxcols: [col]
        ... '''

    JSON::

        >>> json_f = '''
        ... [
        ...     {
        ...         "file": "file1",
        ...         "name": "dst1",
        ...         "idxcols": [
        ...             "cola",
        ...             "colb"
        ...         ]
        ...     },
        ...     {
        ...         "file": "file2",
        ...         "name": "dst2",
        ...         "idxcols": [
        ...             "colx",
        ...             "coly",
        ...             "colz"
        ...         ]
        ...     },
        ...     {
        ...         "file": "file3",
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
             file  name             idxcols
        0   file1  dst1        (cola, colb)
        1   file2  dst2  (colx, coly, colz)
        2   file3  dst3              (col,)

    """
    pkg_dir = Path(fpath).parent
    idx = read_pkg_index(fpath)
    pkg = create_pkg(meta, idx["file"], base_path=f"{pkg_dir}")
    for entry in idx.to_records():
        resource_name = Path(entry.file).stem
        _, update = index_levels(pkg_dir / entry.file, entry.idxcols)
        update_pkg(pkg, resource_name, update)
        update_pkg(pkg, resource_name, {"primaryKey": entry.idxcols}, fields=False)
        # set of value columns
        cols = (
            glom(
                pkg.descriptor,
                (
                    "resources",
                    Iter()
                    .filter(select("path", equal_to=entry.file))
                    .map("schema.fields")
                    .flatten()
                    .map("name")
                    .all(),
                    set,
                ),
            )
            - set(entry.idxcols)
        )
        update_pkg(pkg, resource_name, {col: registry(col, "cols") for col in cols})
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
        pkg.descriptor,
        (
            "resources",
            Iter()
            .filter(select("path", equal_to=row["file"]))
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


def pkg_from_files(meta: Dict, idxpath: _path_t, fpaths: Iterable[_path_t]):
    """Create a package from an index file and other files

    Parameters
    ----------
    meta : Dict
        A dictionary with package metadata.

    idxpath : Union[str, Path]
        Path to the index file.  Note the index file has to be at the top level
        directory of the datapackage.  See :func:`sark.dpkg.read_pkg_index`

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
    pkgdir, pkg, idx = pkg_from_index(meta, idxpath)
    idx_fpath = idx["file"].apply(lambda f: pkgdir / f)  # convert to full path
    _fpaths = [
        _p1.relative_to(pkgdir)  # convert path to relative to pkgdir
        for _p1 in filter(
            lambda _p2: not idx_fpath.apply(_p2.samefile).any(), map(Path, fpaths)
        )  # accept only if not in index
    ]
    pkg = create_pkg(pkg.descriptor, _fpaths, base_path=pkgdir)
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

    Warnings
    --------
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
    dwim_file(files[-1], pkg.descriptor)

    if isinstance(idx, pd.DataFrame):
        files.append(pkgdir / "index.json")
        dwim_file(files[-1], idx.to_dict(orient="records"))

    if isinstance(glossary, pd.DataFrame):
        files.append(pkgdir / "glossary.json")
        dwim_file(files[-1], glossary.to_dict(orient="records"))

    # TODO: support saving to archives (zip, tar, etc)
    return files
