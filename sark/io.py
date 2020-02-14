"""I/O"""

from hashlib import sha256
from pathlib import Path
import tempfile
import time
from typing import Tuple

import requests


def get_cachedir() -> Path:
    """Create the directory `$TMPDIR/sark-cache` and return the Path object"""
    cachedir = Path(tempfile.gettempdir()) / "sark-cache"
    cachedir.mkdir(exist_ok=True)
    return cachedir


class HttpCache:
    """An HTTP cache

    It accepts a URL template which accepts parameters:
    'https://www.example.com/path/{}.json', the parameters can be provided
    later at fetch time.  No checks are made if the number of parameters passed
    are compatible with the URL template.

    After fetching a resource, it is cached in a file under
    '$TMPDIR/sark-cache/'.  The file name is of the form
    'http-<checksum-of-url-template>-<checksum-of-url>'.  The cache is updated
    every 24 hours.  A user may also force a cache cleanup by calling the
    'remove()' method.

    Attributes
    ----------
    cachedir : pathlib.Path
        Path object pointing to the cache directory

    """

    cachedir: Path = get_cachedir()

    def __init__(self, url_t: str):
        """
        Parameters
        ----------
        url_t : str
            URL template, e.g. 'https://www.example.com/path/{}.json'
        """
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
        """"Get the URL contents

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
            raise ValueError(
                f"error: {response.url} responded {response.reason}"
            )
