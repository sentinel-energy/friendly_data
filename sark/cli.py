"""Command line utilities

Commands to manage a data package.

"""

from pathlib import Path
import re
from typing import Dict, List, Union

from glom import glom, Iter
import pandas as pd

from sark._types import _license_t, _path_t
from sark.dpkg import create_pkg
from sark.dpkg import idxpath_from_pkgpath
from sark.dpkg import pkg_from_files
from sark.dpkg import pkg_glossary
from sark.dpkg import read_pkg
from sark.dpkg import read_pkg_index
from sark.dpkg import write_pkg
from sark.helpers import is_windows
from sark.io import dwim_file
from sark.metatools import _fetch_license, check_license


def sanitise(string: str) -> str:
    """Sanitise string for use as group/directory name"""
    return "_".join(re.findall(re.compile("[^ @&()/]+"), string))


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
def _create(meta: Dict, idxpath: _path_t, *fpaths: _path_t) -> str:
    pkgdir, pkg, idx = pkg_from_files(meta, idxpath, fpaths)
    glossary = pkg_glossary(pkg, idx)
    fmeta, fglossary = write_pkg(pkg, pkgdir, glossary=glossary)
    return f"Package metadata: {fmeta}\nPackage glossary: {fglossary}"


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
        Path to the index file.  Note the index file has to be at the top
        level directory of the datapackage.

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
    return _create(meta, idxpath, *fpaths)


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
    _fpaths = [p.relative_to(pkgdir) for p in map(Path, fpaths)]
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
        return _create(meta, idxpath_from_pkgpath(pkgpath), *fpaths)


def _rm_from_pkg(pkgpath: _path_t, *fpaths: _path_t):
    pkg = read_pkg(pkgpath)
    count = len(pkg["resources"])
    resources = glom(
        pkg,
        (
            "resources",
            Iter().filter(lambda r: pkgpath / r["path"] not in map(Path, fpaths)).all(),
        ),
    )
    if count == len(resources):
        return None  # no changes
    pkg["resources"] = resources
    return pkg


def _rm_from_idx(pkgpath: _path_t, *fpaths: _path_t) -> pd.DataFrame:
    pkgpath = Path(pkgpath)
    idx = read_pkg_index(idxpath_from_pkgpath(pkgpath))
    to_rm = idx["file"].apply(
        lambda entry: not any(p.samefile(pkgpath / entry) for p in map(Path, fpaths))
    )
    return idx[to_rm]


def _rm_from_glossary(pkgpath: _path_t, *fpaths: _path_t) -> Union[None, pd.DataFrame]:
    jsonpath = Path(pkgpath) / "glossary.json"
    if not jsonpath.exists():
        return None
    glossary = pd.read_json(jsonpath)
    to_rm = glossary["file"].apply(
        lambda entry: not any(
            p.samefile(jsonpath.parent / entry) for p in map(Path, fpaths)
        )
    )
    return glossary[to_rm]


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
    glossary = _rm_from_glossary(pkgpath, *fpaths)
    if glossary is not None:
        fmeta, fidx, fglossary = write_pkg(pkg, pkgpath, idx=idx, glossary=glossary)
        msgs = [
            f"Package metadata: {fmeta}",
            f"Package index: {fidx}",
            f"Package glossary: {fglossary}",
        ]
    else:
        fmeta, fidx = write_pkg(pkg, pkgpath, idx=idx)
        msgs = [f"Package metadata: {fmeta}", f"Package index: {fidx}"]
    return "\n".join(msgs)


def main():  # pragma: no cover, CLI entry point
    """Entry point for console scripts"""
    import os
    import fire

    os.environ["PAGER"] = "cat"
    fire.Fire({"create": create, "add": add, "update": update, "remove": remove})


if __name__ == "__main__":
    main()
