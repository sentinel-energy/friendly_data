"""I/O"""

import time
import tempfile
from pathlib import Path
from hashlib import sha256
from typing import Tuple

import requests


def get_cachedir() -> Path:
    cachedir = Path(tempfile.gettempdir()) / "sark-cache"
    cachedir.mkdir(exist_ok=True)
    return cachedir


class HttpCache:
    cachedir: Path = get_cachedir()

    def __init__(self, url_t):
        self.url_t = url_t
        self.url_t_hex = sha256(bytes(url_t, "utf8")).hexdigest()

    def cachefile(self, arg: str, *args: str) -> Tuple[Path, str]:
        url = self.url_t.format(arg, *args)
        url_hex = sha256(bytes(url, "utf8")).hexdigest()
        return (
            self.cachedir / f"http-{self.url_t_hex}-{url_hex}",
            url,
        )

    def remove(self, *args: str):
        if len(args):
            files = (i for i in [self.cachefile(*args)[0]])
        else:
            files = self.cachedir.glob(f"http-{self.url_t_hex}-*")
        for cf in files:
            cf.unlink()

    def get(self, arg: str, *args: str) -> bytes:
        cachefile, url = self.cachefile(arg, *args)
        if not cachefile.exists() or (
            time.time() - cachefile.stat().st_ctime > 24 * 3600
        ):
            cachefile.write_bytes(self.fetch(url))
        return cachefile.read_bytes()

    def fetch(self, url: str) -> bytes:
        response = requests.get(url)
        if response.ok:
            return response.content
        else:
            raise ValueError(
                f"error: {response.url} responded {response.reason}"
            )
