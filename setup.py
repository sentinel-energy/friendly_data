"""SENTINEL archive

"""

from pathlib import Path

from setuptools import setup, find_packages

requirements = list(
    filter(
        lambda i: "git://" not in i,
        Path("requirements.txt").read_text().strip().split("\n"),
    )
)

setup(
    name="SENTINEL-archive",
    version="0.1.dev0",
    description="A datapackage implementation for the SENTINEL project.",
    url="https://github.com/sentinel-energy/sentinel-achive",
    packages=find_packages(exclude=["doc", "testing", "tests", "dsts", "tmp"]),
    install_requires=requirements,
)
