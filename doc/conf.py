# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
import sys
from unittest.mock import MagicMock
import sphinx_rtd_theme  # noqa: F401


class MyMock(MagicMock):
    def __repr__(self):
        parent = self._mock_parent
        return f"{parent}.{self._mock_name}" if parent else self._mock_name


sys.path.insert(0, "../")
mocked_modules = ["glom", "glom.matching", "pandas", "pyam", "xarray"]
sys.modules.update((mod, MyMock(name=mod)) for mod in mocked_modules)

# -- Project information -----------------------------------------------------

project = "friendly_data"
copyright = "2021, Suvayu Ali and contributors"
author = "Suvayu Ali"

# The full version, including alpha/beta/rc tags
release = "0.2.2.dev"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
    "sphinx.ext.extlinks",
    "numpydoc",
    # "sphinx_search.extension",
    "sphinx_rtd_theme",
]

autodoc_default_options = {"members": None}
autosummary_generate = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

html_theme_options = {
    "collapse_navigation": False,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]

# -- Options for LaTeX output ------------------------------------------------
latex_engine = "xelatex"

latex_documents = [
    ("index_pdf", "sentinel-data-format.tex", "", "SENTINEL collaboration", "howto"),
    # (
    #     "api/api",
    #     "sentinel-archive-api-docs.tex",
    #     "SENTINEL archive API docs",
    #     "",
    #     "manual",
    # ),
]
