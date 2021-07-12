Frictionless energy data, or ``friendly_data``, is two things:

1. an energy modelling friendly specification to combine data and metadata into a "data package" and
2. a tool to help create, modify and read such data packages.

Its design goal is a common medium to facilitate the flow of data between energy and environmental models in a way that can be automated, while able to deal with the fact that different models use different internal data formats, unit conventions, or variable naming schemes.


A FIGURE THAT SHOWS GRAPHICALLY WHAT A TYPICAL A DATA PACKAGE LOOKS LIKE?


Friendly data packages
----------------------

* A ``friendly_data`` data package is based on and compatible with the `frictionless data package specification <https://frictionlessdata.io/data-package/>`_.
* Additional conventions on top of basic frictionless data packages:
    * Units: bla bla bla bla.
    * Aliases: bla bla bla bla (do these make their way into the data package metadata at all?)

The ``friendly_data`` tool
--------------------------

* Basic use of the ``friendly_data`` tool, which requires no Python experience, makes it easy to create and manage data packages.
* An online metadata registry allows teams to share and agree on variable names and definitions and makes the generation of metadata for a data package quick and easy.
* Automated conversion to and from the `IAMC timeseries scenario data format <https://pyam-iamc.readthedocs.io/en/stable/data.html>`_.
* Written in Python, thus linking directly to the rich Python ecosystem of data analysis and visualisation tools.
* Python programmers can make use of the API to further automate tasks related to creating, validating, and using ``friendly_data`` packages.
