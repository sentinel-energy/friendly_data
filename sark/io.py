"""Functions useful for I/O and file manipulation

"""

from hashlib import sha256
import json
from pathlib import Path
import tempfile
import time
from typing import Dict, Iterable, List, overload, Tuple, Union

import requests
import yaml

from sark._types import _path_t


def relpaths(basepath: _path_t, pattern: Union[str, Iterable[_path_t]]) -> List[str]:
    """Convert a list of paths to relative paths

    Parameters
    ----------
    basepath : Union[str, Path]
        Path to use as the reference when calculating relative paths
    pattern : Union[str, Iterable[Union[str, Path]]]
        Either a pattern relative to ``basepath`` to generate a list of paths,
        or a list of paths to convert.

    Returns
    -------
    List[str]
        List of relative paths (as ``str``-s)

    """
    if isinstance(pattern, str):
        basepath = Path(basepath)
        return [str(p.relative_to(basepath)) for p in basepath.glob(pattern)]
    else:  # iterable of "paths"
        return [str(p.relative_to(basepath)) for p in map(Path, pattern)]


def path_in(fpaths: Iterable[_path_t], testfile: _path_t) -> bool:
    """Function to test if a path is in a list of paths.

    The test checks if they are the same physical files or not, so the testfile
    needs to exist on disk.

    Parameters
    ----------
    fpaths : Iterable[Union[str, Path]]
        List of paths to check
    testfile : Union[str, Path]
        Test file (must exist on disk)

    Returns
    -------
    bool

    """
    return any(p.samefile(testfile) for p in map(Path, fpaths))


def path_not_in(fpaths: Iterable[_path_t], testfile: _path_t) -> bool:
    """Function to test if a path is absent from a list of paths.

    Opposite of :func:`~sark.io.path_in`.

    Parameters
    ----------
    fpaths : Iterable[Union[str, Path]]
        List of paths to check
    testfile : Union[str, Path]
        Test file (must exist on disk)

    Returns
    -------
    bool

    """
    return not path_in(fpaths, testfile)


def posixpathstr(fpath: _path_t) -> str:
    """Given a path object, return a POSIX compatible path string

    Parameters
    ----------
    fpath : Unioin[str, Path]
        Path object

    Returns
    -------
    str

    """
    return str(Path(fpath).as_posix())


@overload
def dwim_file(fpath: _path_t) -> Union[Dict, List]:
    ...  # pragma: no cover, oveload


@overload
def dwim_file(fpath: _path_t, data) -> None:
    ...  # pragma: no cover, oveload


def dwim_file(fpath: _path_t, data=None):
    """Do What I Mean with file

    Depending on the function arguments, either read the contents of a file, or
    write data to the file.  The file type is guessed from the extension;
    supported formats: JSON and YAML.

    Parameters
    ----------
    fpath : Union[str, Path]
        File path to read or write to

    data : Union[None, Any]
        Data, when writing to a file.

    Returns
    -------
    Union[None, Union[Dict, List]]
        - If writing to a file, nothing (``None``) is returned
        - If reading from a file, depending on the contents, either a list or
          dictionary are returned

    """
    fpath = Path(fpath)
    mode = "r" if data is None else "w"
    if fpath.suffix in (".yaml", ".yml"):
        with open(fpath, mode=mode) as stream:
            if data is None:
                return yaml.safe_load(stream)
            else:
                yaml.safe_dump(data, stream)
    elif fpath.suffix == ".json":
        with open(fpath, mode=mode) as stream:
            if data is None:
                return json.load(stream)
            else:
                json.dump(data, stream, indent=4)
    else:
        raise RuntimeError(f"{fpath}: not a JSON or YAML file")


def get_cachedir() -> Path:
    """Create the directory ``$TMPDIR/sark-cache`` and return the Path object"""
    cachedir = Path(tempfile.gettempdir()) / "sark-cache"
    cachedir.mkdir(exist_ok=True)
    return cachedir


class HttpCache:
    """An HTTP cache

    It accepts a URL template which accepts parameters:
    ``https://www.example.com/path/{}.json``, the parameters can be provided
    later at fetch time.  No checks are made if the number of parameters passed
    are compatible with the URL template.

    After fetching a resource, it is cached in a file under
    ``$TMPDIR/sark-cache/``.  The file name is of the form
    ``http-<checksum-of-url-template>-<checksum-of-url>``.  The cache is
    updated every 24 hours.  A user may also force a cache cleanup by calling
    the :meth:`remove()` method.

    Parameters
    ----------
    url_t : str
        URL template, e.g. ``https://www.example.com/path/{}.json``

    Attributes
    ----------
    cachedir : pathlib.Path
        Path object pointing to the cache directory

    """

    cachedir: Path = get_cachedir()

    def __init__(self, url_t: str):
        self.url_t = url_t
        self.url_t_hex = sha256(bytes(url_t, "utf8")).hexdigest()

    def cachefile(self, arg: str, *args: str) -> Tuple[Path, str]:
        """Return the cache file, and the corresponding URL

        Parameters
        ----------
        arg : str
            parameters for the URL template (one mandatory)
        *args : str, optional
            more parameters (optional)

        Returns
        -------
        Tuple[pathlib.Path, str]
            Tuple of Path object pointing to the cache file and the URL string

        """
        url = self.url_t.format(arg, *args)
        url_hex = sha256(bytes(url, "utf8")).hexdigest()
        return (
            self.cachedir / f"http-{self.url_t_hex}-{url_hex}",
            url,
        )

    def remove(self, *args: str):
        """Remove cache files

        - Remove all files associated with this cache (w/o arguments).
        - Remove only the files associated with the URL formed from the args.

        Parameters
        ----------
        *args : str, optional
            parameters for the URL template

        Raises
        ------
        FileNotFoundError
            If an argument is provided to remove a specific cache file, but the
            cache file does not exist.

        """
        if len(args):
            files = (i for i in [self.cachefile(*args)[0]])
        else:
            files = self.cachedir.glob(f"http-{self.url_t_hex}-*")
        for cf in files:
            cf.unlink()

    def get(self, arg: str, *args: str) -> bytes:
        """Get the URL contents

        If a valid cache exists, return the contents from there, otherwise
        fetch again.

        Parameters
        ----------
        arg : str
            parameters for the URL template (one mandatory)
        *args : str, optional
            more parameters (optional)

        Returns
        -------
        bytes
            bytes array of the contents

        Raises
        ------
        ValueError
            If the URL is incorrect
        requests.ConnectionError
            If there is no network connection

        """
        cachefile, url = self.cachefile(arg, *args)
        if not cachefile.exists() or (
            time.time() - cachefile.stat().st_ctime > 24 * 3600
        ):
            cachefile.write_bytes(self.fetch(url))
        return cachefile.read_bytes()

    def fetch(self, url: str) -> bytes:
        """Fetch the URL

        Parameters
        ----------
        url : str
            URL to fetch

        Returns
        -------
        bytes
            bytes array of the contents that was fetched

        Raises
        ------
        ValueError
            If the URL is incorrect

        """
        response = requests.get(url)
        if response.ok:
            return response.content
        else:
            raise ValueError(f"error: {response.url} responded {response.reason}")
