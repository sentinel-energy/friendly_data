Frictionless Energy data - Friendly data
========================================
|unittests| |coverage| |docs|

A frictionless_ data package implementation for energy data;
maintained by the `SENTINEL collaboration`_.

This package provides a Python API and CLI utilities to read/write and
manage Energy (systems) data as a frictionless data package.  The
Python API includes easy conversion to and from standard Python data
structures common in data analysis; e.g. ``pandas.DataFrame``,
``xarray.DataArray``, ``xarray.Dataset``, etc.

It introduces energy modelling terminology by relying on an external
(but loosely coupled) registry_ that collates commonly used variables
and associated metadata.  If someone outside of the energy modelling
community wants to make use this package, it should be relatively
striaghtforward to point to a different registry while continuing to
use the same workflow.  Read more in the documentation_.

.. _frictionless:
   https://frictionlessdata.io/

.. _`SENTINEL collaboration`:
   https://sentinel.energy/

.. _registry:
   https://github.com/sentinel-energy/friendly_data_registry

.. _documentation:
   https://sentinel-energy.github.io/friendly_data/

.. |unittests| image:: https://github.com/sentinel-energy/friendly_data/workflows/Unit%20tests/badge.svg
   :target: https://github.com/sentinel-energy/friendly_data/actions?query=workflow%3A%22Unit+tests%22

.. |coverage| image:: https://codecov.io/gh/sentinel-energy/friendly_data/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/sentinel-energy/friendly_data

.. |docs| image:: https://github.com/sentinel-energy/friendly_data/workflows/Publish%20docs/badge.svg
  :target: https://github.com/sentinel-energy/friendly_data/actions?query=workflow%3A%22Publish+docs%22


Installation
------------

You can install (or update) the package with ``pip``::

  $ pip install [-U] friendly-data
    
As the registry is coupled loosely, so that it can be updated much
faster.  This will accomodate to an evolving data vocabulary of the
community.  So it is recommended to update the registry regularly, eventhough you might not want to update the main package.  You can do this with::

  $ pip install -U friendly-data-registry

If your workflow also includes converting your data packages into IAMC
format, you will need the optional dependencies; which can be
installed at any time like this (leaving out the `-U` will not upgrade
the main package)::

  $ pip install [-U] friendly-data[extras]
