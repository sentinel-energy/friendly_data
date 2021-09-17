Frictionless energy data, or ``friendly_data``, is two things:

1. an energy modelling friendly specification to combine data and metadata into a "data package" and
2. a tool to help create, modify and read such data packages.

Its design goal is a common medium to facilitate the flow of data between energy and environmental models in a way that can be automated, while able to deal with the fact that different models use different internal data formats, unit conventions, or variable naming schemes.

.. figure:: _static/images/friendly_data_schematic_alt.png
   :width: 90%
   :align: center

   A typical data package consists of a collection of datasets, and
   related metadata.  There are two kinds of metadata: a) package wide
   semantic information providing context, terms of use, etc, and b)
   structural information about the included datasets.


Friendly data packages
----------------------

A ``friendly_data`` data package is based on, and is compatible with
the `frictionless data package specification
<https://frictionlessdata.io/data-package/>`_.  The ``friendly_data``
tool adds the following features on top of the basic frictionless
specification:

- *Aliases*: you can specify column aliases to indicate two different
  column names are equivalent.  This reduces friction due to varying
  terminology used by different groups/sub-communities.
- *Units*: **TODO**

The ``friendly_data`` tool
--------------------------

- Basic use of the ``friendly_data`` tool, which requires no Python
  experience, makes it easy to create and manage data packages.
- An online metadata registry allows teams to share and agree on
  variable names and definitions and makes the generation of metadata
  for a data package quick and easy.
- Automated conversion to and from the `IAMC timeseries scenario data
  format <https://pyam-iamc.readthedocs.io/en/stable/data.html>`_.
- Written in Python, with a library API:

  - thus linking directly to the rich Python ecosystem of data
    analysis and visualisation tools,
  - programmers can also make use of the API to further automate tasks
    related to creating, validating, and using ``friendly_data``
    packages.
