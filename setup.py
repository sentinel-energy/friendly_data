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

extra_requirements = {
    "extras": Path("requirements-extras.txt").read_text().strip().split("\n")
}

setup(
    name="friendly_data",
    version="v0.3.3",
    description="A frictionless data package implementation for energy system data.",
    long_description=Path("README.rst").read_text(),
    long_description_content_type="text/x-rst",
    url="https://github.com/sentinel-energy/friendly_data",
    packages=find_packages(exclude=["doc", "testing", "tests", "dsts", "tmp"]),
    install_requires=requirements,
    extra_requirements=extra_requirements,
    package_data={"friendly_data": ["py.typed", "doc/*.template"]},
    entry_points={
        "console_scripts": [
            "friendly_data = friendly_data.cli:main",
            "friendly-data = friendly_data.cli:main",
        ]
    },
)
