"""Functions that are run from the CLI to create, or edit a data package.

"""

from datetime import datetime
from itertools import chain
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List

from frictionless import Package
from glom import glom, Iter

from friendly_data import logger_config
from friendly_data._types import _license_t, _path_t
from friendly_data.converters import to_df
from friendly_data.dpkg import entry_from_res
from friendly_data.dpkg import idxpath_from_pkgpath
from friendly_data.dpkg import pkg_from_files
from friendly_data.dpkg import pkgindex
from friendly_data.dpkg import read_pkg
from friendly_data.dpkg import set_idxcols
from friendly_data.dpkg import write_pkg
from friendly_data.helpers import consume, filter_dict
from friendly_data.helpers import is_windows
from friendly_data.helpers import sanitise
from friendly_data.io import copy_files
from friendly_data.io import dwim_file
from friendly_data.io import path_not_in
from friendly_data.io import outoftree_paths
from friendly_data.metatools import _fetch_license
from friendly_data.metatools import check_license
from friendly_data.metatools import get_license
from friendly_data.metatools import lic_metadata
from friendly_data.metatools import resolve_licenses
from friendly_data.registry import config_ctx
from friendly_data.doc import get_template, page

logger = logger_config(fmt="{name}: {levelname}: {message}")


def list_licenses() -> str:
    """List commonly used licenses

    NOTE: for Python API users, not to be confused with
    :func:`metatools.list_licenses`.

    Returns
    -------
    str
        ASCII table with commonly used licenses

    """
    from tabulate import tabulate

    keys = ("domain", "id", "maintainer", "title")
    return tabulate(lic_metadata(keys), headers="keys")


def license_info(lic: str) -> Dict:
    """Give detailed metadata about a license

    Parameters
    ----------
    lic : str
        License ID as listed in the output of ``friendly_data list-licenses``

    Returns
    -------
    Dict
        License metadata

    """
    keys = ("domain", "id", "maintainer", "title", "url")
    lic_info = lic_metadata(keys, lambda i: i["id"] == lic)
    if not lic_info:
        logger.error(f"no matching license with id: {lic}")
        sys.exit(1)
    return lic_info[0]


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
    name: str = "",
    title: str = "",
    licenses: str = "",
    description: str = "",
    keywords: str = "",
    config: _path_t = "",
) -> Dict:
    """Metadata from the config file is overriden by keyword arguments"""
    if config:
        try:
            meta = dwim_file(config)["metadata"]  # type: ignore[call-overload]
        except KeyError as err:
            logger.warning(f"{err}: section missing from {config}")
            meta = {}
        else:
            meta = resolve_licenses(meta)
    else:
        meta = {}
    _meta: Dict[str, Any] = {
        "name": name if name else sanitise(title),
        "title": title,
        "description": description,
        "keywords": keywords.split(),
    }
    if licenses:
        _meta["licenses"] = [get_license(licenses)]
    elif "licenses" in mandatory and "licenses" not in meta:
        _meta["licenses"] = [license_prompt()]  # pragma: no cover

    # override config file with values from flags
    meta.update((k, v) for k, v in _meta.items() if v)

    check = [k for k in mandatory if k not in meta]  # mandatory fields
    if check:
        logger.error(f"{check}: mandatory metadata missing")
        if "license" in meta:
            logger.error("'license': should be plural!")
        sys.exit(1)

    return meta


# TODO: ability to add datasets from arbitrary paths
# - flag to provide destination directory for out of tree datasets
# - normalise relative path w.r.t. index entries
# - for files not in the index, normalise relative path w.r.t. pkgdir
# add similar ability for update(..)
def _create(
    meta: Dict,
    pkgpath: _path_t,
    fpaths: Iterable[_path_t],
    *,
    export: _path_t,
) -> List[Path]:
    if export:
        pkgpath, export = Path(pkgpath), Path(export)
        idxp = idxpath_from_pkgpath(pkgpath) if pkgpath.is_dir() else pkgpath
        spec = Iter("path").map(lambda p: idxp.parent / p)  # type: ignore[union-attr]
        if idxp:  # create a uniquified list of files
            files = chain(
                [idxp],
                set(chain(glom(pkgindex.from_file(idxp), spec), map(Path, fpaths))),
            )
        else:
            files = fpaths  # type: ignore[assignment]
        # NOTE: if idxpath was found, first of the returned files is the index
        # file that was copied in the export directory, extract it to pkgpath
        fpaths = copy_files(files, export, pkgpath)
        if idxp:
            pkgpath, *fpaths = fpaths
        else:  # if no index was found, set export directory to new pkgpath
            pkgpath = export

    pkgdir, pkg, _ = pkg_from_files(meta, pkgpath, fpaths)
    return write_pkg(pkg, pkgdir)


def create(
    idxpath: str,
    *fpaths: str,
    name: str = "",
    title: str = "",
    licenses: str = "",
    description: str = "",
    keywords: str = "",
    inplace: bool = False,
    export: str = "",
    config: str = "",
):
    """Create a package from an index file and other files

    Package metadata provided with command line flags override metadata from
    the config file.

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

    licenses : str
        License

    description : str
        Package description

    keywords : str
        A space separated list of keywords: 'renewable energy model' ->
        ['renewable', 'energy', 'model']

    inplace : bool
        Whether to create the data package by only adding metadata to the
        current directory.  NOTE: one of inplace/export must be chosen

    export : str
        Create the data package in the provided directory instead of the
        current directory

    config : str
        Config file in YAML format with metadata and custom registry.  The
        metadata should be under a "metadata" section, and the custom registry
        under a "registry" section.

    """
    if (not export) and (not inplace):
        logger.error("you must explicitly choose between `inplace` or `export`")
        sys.exit(1)
    elif export and inplace:
        logger.warning(
            "both `inplace` and `export` present, `inplace` will be ignored`"
        )

    meta = {
        "name": name,
        "title": title,
        "licenses": licenses,
        "description": description,
        "keywords": keywords,
        "config": config,
    }
    meta = _metadata(["name", "licenses"], **meta)  # type: ignore[arg-type]
    with config_ctx(conffile=config):
        files = _create(meta, idxpath, fpaths, export=export)
    return f"Package metadata: {files[0]}"


def _update(pkg: Dict, pkgpath: _path_t, fpaths: Iterable[_path_t]):
    _fpaths1, outoftree = outoftree_paths(pkgpath, fpaths)
    _fpaths2 = copy_files(outoftree, pkgpath)
    fpaths = _fpaths1 + _fpaths2
    pkg = _rm_from_pkg(pkg, pkgpath, fpaths)
    return _create(pkg, pkgpath, fpaths, export="")


# TODO: option to update files in index
def update(
    pkgpath: str,
    *fpaths: str,
    name: str = "",
    title: str = "",
    licenses: str = "",
    description: str = "",
    keywords: str = "",
    config: str = "",
):
    """Update metadata and datasets in a package.

    Parameters
    ----------
    pkgpath : str
        Path to the package.

    fpaths : Tuple[str]
        List of datasets/resources; they could be new datasets or datasets with
        updated index entries.

    name : str
        Package name (no spaces or special characters)

    title : str
        Package title

    description : str
        Package description

    keywords : str
        A space separated list of keywords: 'renewable energy model' ->
        ['renewable', 'energy', 'model']

    licenses : str
        License

    config : str
        Config file in YAML format with metadata and custom registry.  The
        metadata should be under a "metadata" section, and the custom registry
        under a "registry" section.

    """
    meta = {
        "name": name,
        "title": title,
        "licenses": licenses,
        "description": description,
        "keywords": keywords,
        "config": config,
    }
    meta = _metadata([], **meta)  # type: ignore[arg-type]
    pkg = read_pkg(pkgpath)
    pkg.update(meta)

    if len(fpaths) == 0:
        files = write_pkg(pkg, pkgpath)
    else:
        with config_ctx(conffile=config):
            files = _update(pkg, pkgpath, fpaths)
    return f"Package metadata: {files[0]}"


def _rm_paths_spec(pkgpath: _path_t, fpaths: Iterable[_path_t]):
    pkgpath = Path(pkgpath)
    return Iter().filter(lambda r: path_not_in(fpaths, pkgpath / r["path"])).all()


def _rm_from_pkg(pkg: Dict, pkgpath: _path_t, fpaths: Iterable[_path_t]):
    count = len(pkg["resources"])
    pkg["resources"] = glom(pkg["resources"], _rm_paths_spec(pkgpath, fpaths))
    if count == len(pkg["resources"]):
        logger.info("no resources to update/remove in package")
    return pkg


def _rm_from_idx(pkgpath: _path_t, fpaths: Iterable[_path_t]) -> pkgindex:
    idx = pkgindex.from_file(idxpath_from_pkgpath(pkgpath))
    return glom(idx, _rm_paths_spec(pkgpath, fpaths))


def _rm_from_disk(fpaths: Iterable[_path_t]):
    consume(map(lambda fp: Path(fp).unlink(), fpaths))


def remove(pkgpath: str, *fpaths: str, rm_from_disk: bool = False) -> str:
    """Remove datasets from the package

    Parameters
    ----------
    pkgpath : str
        Path to the package directory

    fpaths : Tuple[str]
        List of datasets/resources to be removed from the package. The index is
        updated accordingly.

    rm_from_disk : bool (default: False)
        Permanently delete the files from disk

    """
    pkg = _rm_from_pkg(read_pkg(pkgpath), pkgpath, fpaths)
    idx = _rm_from_idx(pkgpath, fpaths)
    fmeta, fidx = write_pkg(pkg, pkgpath, idx=idx)
    if rm_from_disk:
        _rm_from_disk(fpaths)
    msgs = [f"Package metadata: {fmeta}", f"Package index: {fidx}"]
    return "\n".join(msgs)


def generate_index_file(idxpath: str, *fpaths: str, config: str = ""):
    """Generate an index file from a set of dataset files

    Parameters
    ----------
    idxpath : str
        Path where the index file (YAML format) should be written

    fpaths : Tuple[str]
        List of datasets/resources to include in the index

    config : str
        Config file in YAML format with custom registry.  It should be defined
        under a "registry" section.

    """
    with config_ctx(conffile=config):
        idx = [entry_from_res(set_idxcols(f)) for f in fpaths]
    dwim_file(idxpath, idx)


def to_iamc(config: str, idxpath: str, iamcpath: str, *, wide: bool = False):
    """Aggregate datasets into an IAMC dataset

    Parameters
    ----------
    config : str
        Config file

    idxpath : str
        Index file

    iamcpath : str
        IAMC dataset

    wide : bool (default: False)
        Enable wide IAMC format

    """
    from friendly_data.iamc import IAMconv

    conv = IAMconv.from_file(config, idxpath)
    files = conv.res_idx.get("path")
    conv.to_csv(files, output=iamcpath, wide=wide)
    return f"{', '.join(files)} -> {iamcpath}"


def reports(pkg: Package, report_dir: str):
    """Write HTML reports summarising all resources in the package

    Parameters
    ----------
    pkg : Package

    report_dir : str
        Directory where reports are written

    Returns
    -------
    int
        Bytes written (index.html)
    """
    import pandas_profiling as _  # noqa: F401

    _dir = Path(report_dir)
    _dir.mkdir(parents=True, exist_ok=True)

    title = pkg.get("title", pkg["name"])
    res = {"title": title, "date": datetime.now().isoformat(), "resources": []}
    for _res in pkg.resources:
        df = to_df(_res, noexcept=True)
        html = Path(_res["path"]).with_suffix(".html")
        report = df.profile_report(
            title=title,
            dataset=filter_dict(_res, ["description"]),  # FIXME: add pkg url
            variables={
                "descriptions": glom(
                    _res,
                    (
                        "schema.fields",
                        Iter()
                        .filter(lambda i: "description" in i)
                        .map(lambda i: (i["name"], i["description"]))
                        .all(),
                        dict,
                    ),
                )
            },
        )
        report.config.html.minify_html = False
        report.to_file(_dir / html)
        res["resources"].append({"path": html, "name": _res["name"]})

    tmpl = get_template("index.html.template")
    return (_dir / "index.html").write_text(tmpl.render(res))


def describe(pkgpath: str, config: str = "", report_dir: str = ""):
    """Give a summary of the data package

    Parameters
    ----------
    pkgpath : str
        Path to the data package

    report_dir : str (default: empty string)
        If not empty, generate an HTML report and write to the directory
        ``report``.  The directory will have an ``index.html`` file, and one
        HTML file for each dataset.

    config : str
        Config file in YAML format with custom registry.  It should be defined
        under a "registry" section.

    """
    try:
        pkg = read_pkg(pkgpath)
    except (ValueError, FileNotFoundError):
        sys.exit(1)
    res = {}
    meta_f = ("name", "title", "description", "keywords", "licenses")
    for k, v in pkg.items():
        if k in meta_f and v:
            res[k] = glom(v, ["name"]) if k == "licenses" else v

    if report_dir:
        res["report_dir"] = report_dir
        with config_ctx(conffile=config):
            reports(pkg, report_dir)

    tmpl = get_template("dpkg_describe.template")
    res["resources"] = glom(
        pkg["resources"], [{"fields": ("schema.fields", ["name"]), "path": "path"}]
    )
    return tmpl.render(res)


def describe_registry(column_type: str = ""):
    """Describe columns defined in the registry

    Parameters
    ----------
    column_type : str (default: empty string → all)
        Column type to list; one of: "cols", or "idxcols".  If nothing is
        provided (default), columns of both types are listed.

    """
    from rich.console import Console
    from rich.markdown import Markdown

    console = Console()
    md = Markdown(page(markup="md", col_t=column_type))
    console.print(md)


def main():  # pragma: no cover, CLI entry point
    """Entry point for console scripts"""
    import os
    import fire

    os.environ["PAGER"] = "cat"
    fire.Fire(
        {
            "create": create,
            "update": update,
            "remove": remove,
            "describe-registry": describe_registry,
            "list-licenses": list_licenses,
            "license-info": license_info,
            "generate-index-file": generate_index_file,
            "to-iamc": to_iamc,
            "describe": describe,
        }
    )
