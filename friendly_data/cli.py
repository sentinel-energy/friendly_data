"""Functions that are run from the CLI to create, or edit a data package.

"""

from pathlib import Path
from typing import Dict, Iterable, List

from glom import glom, Iter
import pandas as pd

from friendly_data._types import _license_t, _path_t
from friendly_data.dpkg import create_pkg
from friendly_data.dpkg import idxpath_from_pkgpath
from friendly_data.dpkg import pkg_from_files
from friendly_data.dpkg import read_pkg
from friendly_data.dpkg import pkgindex
from friendly_data.dpkg import write_pkg
from friendly_data.helpers import is_windows, sanitise
from friendly_data.io import dwim_file, path_not_in, relpaths
from friendly_data.metatools import _fetch_license, check_license
from friendly_data.doc import page


def license_prompt() -> _license_t:  # pragma: no cover, interactive function
    """Prompt for a license on the terminal (with completion)."""
    licenses = _fetch_license("all")

    def complete(text, state):
        for lic in licenses:
            if lic.startswith(text):
                if not state:
                    return lic
                else:
                    state -= 1

    if not is_windows():
        import readline

        readline.parse_and_bind("tab: complete")
        readline.set_completer(complete)

    return check_license(licenses[input("license: ")])


def _metadata(
    mandatory: List[str],
    *,
    name: str,
    title: str,
    license: str,
    description: str,
    keywords: str,
    metadata: _path_t = "",
) -> Dict:
    if metadata:
        meta = dwim_file(Path(metadata))
    else:
        meta = {
            "name": name if name else sanitise(title),
            "title": title,
            "description": description,
            "keywords": keywords.split(),
            "license": license
            if license or "license" not in mandatory
            else license_prompt(),
        }
    meta = {k: meta[k] for k in filter(meta.__getitem__, meta)}

    check = [k for k in mandatory if k not in meta]  # mandatory fields
    if check:
        raise ValueError(f"{check}: mandatory metadata missing")

    return meta


# TODO: ability to add datasets from arbitrary paths
# - flag to provide destination directory for out of tree datasets
# - normalise relative path w.r.t. index entries
# - for files not in the index, normalise relative path w.r.t. pkgdir
# add similar ability for update(..)
def _create(
    meta: Dict,
    idxpath: _path_t,
    fpaths: Iterable[_path_t],
) -> str:
    pkgdir, pkg, idx = pkg_from_files(meta, idxpath, fpaths)
    fmeta, *fidx = write_pkg(pkg, pkgdir, idx=idx)
    msg = f"Package metadata: {fmeta}"
    if idx:
        msg += "\nPackage index: {fidx[0]}"
    return msg


def create(
    idxpath: str,
    *fpaths: str,
    name: str = "",
    title: str = "",
    license: str = "",
    description: str = "",
    keywords: str = "",
    metadata: _path_t = "",
):
    """Create a package from an index file and other files

    Parameters
    ----------
    idxpath : str
        Path to the index file or package directory with the index file.  Note
        the index file has to be at the top level directory of the datapackage.

    fpaths : Tuple[str]
        List of datasets/resources not in the index.  If any of them point to a
        dataset already present in the index, it is ignored.

    name : str
        Package name (no spaces or special characters)

    title : str
        Package title

    description : str
        Package description

    keywords : str
        A space separated list of keywords: 'renewable energy model' ->
        ['renewable', 'energy', 'model']

    license : str
        License

    metadata : str
        Instead of passing metadata via flags, you may provide the metadata as
        JSON or YAML

    """
    meta = {
        "name": name,
        "title": title,
        "license": license,
        "description": description,
        "keywords": keywords,
        "metadata": metadata,
    }
    meta = _metadata(["name", "license"], **meta)  # type: ignore[arg-type]
    return _create(meta, idxpath, fpaths)


def add(pkgpath: str, *fpaths: str):
    """Add datasets to a package

    Parameters
    ----------
    pkgpath : str
        Path to the package.

    fpaths : Tuple[str]
        List of datasets/resources not in the package.  If any of them point to
        a dataset already present in the index, it is ignored.

    """
    pkg = read_pkg(pkgpath)
    pkgdir = Path(pkg.basepath)
    _fpaths = relpaths(pkgdir, fpaths)
    pkg = create_pkg(pkg, _fpaths, basepath=pkg.basepath)
    pkgjson = pkgdir / "datapackage.json"
    dwim_file(pkgjson, pkg)
    return f"Package metadata: {pkgjson}"


# TODO: option to update files in index
def update(
    pkgpath: str,
    *fpaths: str,
    name: str = "",
    title: str = "",
    license: str = "",
    description: str = "",
    keywords: str = "",
    metadata: _path_t = "",
):
    """Update only the metadata of a package.

    Parameters
    ----------
    pkgpath : str
        Path to the package.

    fpaths : Tuple[str]
        List of datasets/resources in the index, that were updated.

    name : str
        Package name (no spaces or special characters)

    title : str
        Package title

    description : str
        Package description

    keywords : str
        A space separated list of keywords: 'renewable energy model' ->
        ['renewable', 'energy', 'model']

    license : str
        License

    metadata : str
        Instead of passing metadata via flags, you may provide the metadata as
        JSON or YAML

    """
    meta = {
        "name": name,
        "title": title,
        "license": license,
        "description": description,
        "keywords": keywords,
        "metadata": metadata,
    }
    meta = _metadata([], **meta)  # type: ignore[arg-type]
    pkg = read_pkg(pkgpath)
    pkg.update(meta)

    if len(fpaths) == 0:
        files = write_pkg(pkg, pkgpath)
        return f"Package metadata: {files[0]}"
    else:
        meta = {k: v for k, v in pkg.items() if k not in ("resources", "profile")}
        return _create(meta, idxpath_from_pkgpath(pkgpath), fpaths)


def _rm_from_pkg(pkgpath: _path_t, *fpaths: _path_t):
    pkg = read_pkg(pkgpath)
    count = len(pkg["resources"])
    resources = glom(
        pkg,
        (
            "resources",
            Iter().filter(lambda r: path_not_in(fpaths, pkgpath / r["path"])).all(),
        ),
    )
    if count == len(resources):
        return None  # no changes
    pkg["resources"] = resources
    return pkg


def _rm_from_idx(pkgpath: _path_t, *fpaths: _path_t) -> pkgindex:
    pkgpath = Path(pkgpath)
    idx = pkgindex.from_file(idxpath_from_pkgpath(pkgpath))
    return glom(
        idx, Iter().filter(lambda r: path_not_in(fpaths, pkgpath / r["path"])).all()
    )


def remove(pkgpath: str, *fpaths: str) -> str:
    """Remove datasets from the package

    Parameters
    ----------
    pkgpath : str
        Path to the package directory

    fpaths : str
        List of datasets/resources to be removed from the package. The index is
        updated accordingly.

    """
    pkg = _rm_from_pkg(pkgpath, *fpaths)
    if pkg is None:
        return "Nothing to do"
    idx = _rm_from_idx(pkgpath, *fpaths)
    fmeta, fidx = write_pkg(pkg, pkgpath, idx=idx)
    msgs = [f"Package metadata: {fmeta}", f"Package index: {fidx}"]
    return "\n".join(msgs)


def main():  # pragma: no cover, CLI entry point
    """Entry point for console scripts"""
    import os
    import fire

    os.environ["PAGER"] = "cat"
    fire.Fire(
        {
            "create": create,
            "add": add,
            "update": update,
            "remove": remove,
            "registry": page,
        }
    )
