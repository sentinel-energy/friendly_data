.. SENTINEL archive documentation master file, created by
   sphinx-quickstart on Tue May 19 16:39:00 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to SENTINEL archive's documentation!
============================================

The SENTINEL partners work with a variety of models in diverse
computing environments.  The **SENTINEL archive** data format has been
chosen to facilitate interoperability between these diverse modeling
frameworks by making sharing of data easier.  Since we have to cope
with a variety of research requirements, and workflows, the format is
fairly flexible, and to a great extent self-descriptive.  We rely on
the authour of a dataset to also describe it.  A typical description
of a dataset includes both metadata and structural information.

Metadata typically consists of:

- a computer programme friendly name,
- a title and free-form descriptive text,
- search keywords,
- license information, and
- citation information.

Whereas structural information should describe the type information,
any contraints, and assumptions implicit in the data.  Our
implementation builds on top of the *frictionless datapackage*
specification.

What is a ``datapackage``?
--------------------------

A datapackage consists of a set of data files, any related source
code, relevant licenses, and a ``datapackage.json`` file that records
all this information in a single place.  The details of the
specification can be found on the website_ for frictionless data.

.. _website: https://specs.frictionlessdata.io/

.. toctree::
   :maxdepth: 2
   :caption: A Hands-on Overview:

   tutorial/metadata
   tutorial/archive
   tutorial/pandas

API documentation
-----------------

.. toctree::
   :maxdepth: 1

   api/api

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
