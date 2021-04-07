"""Frictionless Energy Data

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
    name="friendly_data",
    version="0.1.dev1",
    description="A frictionless data package implementation for energy system data.",
    url="https://github.com/sentinel-energy/friendly_data",
    packages=find_packages(exclude=["doc", "testing", "tests", "dsts", "tmp"]),
    install_requires=requirements,
    entry_points={"console_scripts": ["friendly_data = friendly_data.cli:main"]},
)
