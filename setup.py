"""SENTINEL archive

"""

from setuptools import setup, find_packages

setup(
    name="SENTINEL-archive",
    version="0.1-dev0",
    description="A datapackage implementation for the SENTINEL project.",
    url="https://github.com/sentinel-energy/sentinel-achive",
    packages=find_packages(exclude=["doc", "testing", "tests", "dst", "tmp"]),
    install_requires=[
        "datapackage",
        "flake8",
        "glom",
        "goodtables",
        "mypy",
        "mypy_extensions",
        "numexpr",
        "numpy",
        "pandas",
        "pytest",
        "requests",
        "tableschema",
        "tabulator",
        "xarray",
    ],
)
