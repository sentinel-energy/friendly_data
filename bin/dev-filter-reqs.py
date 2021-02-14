#!/usr/bin/env python

from argparse import ArgumentParser
from pathlib import Path
import re

parser = ArgumentParser()
parser.add_argument("file")


PAT = re.compile("(frictionless).+")
NL = "\n"


def _filter(row):
    match = PAT.match(row)
    if match:
        return match.group(1)
    else:
        return row


if __name__ == "__main__":
    opts = parser.parse_args()
    req = Path(opts.file)
    tmp = req.with_suffix(".tmp")
    tmp.write_text(NL.join([_filter(l) for l in req.read_text().split(NL)]))
    tmp.replace(req)
