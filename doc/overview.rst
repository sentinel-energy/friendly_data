The SENTINEL archive data format
--------------------------------

The SENTINEL partners work with a variety of models in diverse
computing environments.  The **SENTINEL archive** data format [#]_ has
been chosen to facilitate interoperability between these diverse
modelling frameworks by making sharing of data easier.  Since we have
to cope with a variety of research requirements, and workflows, the
format is fairly flexible, and to a great extent self-descriptive.  We
rely on the author of a dataset to also describe it.  A typical
description of a dataset includes both metadata and structural
information.

Metadata of a dataset typically establishes the context for the
dataset.  It can consist of properties like:

- a computer program friendly nameso that they can be referred to
  easily from software,
- a title and free-form descriptive text so that other researchers
  using the dataset are aware of its provenance and use it correctly,
- search keywords for easier discoverability on online platforms,
- license information so that others know the terms of use of the
  data, and
- citation information.

Whereas structural information should describe the type information,
any contraints, and assumptions implicit in the data.  Our
implementation builds on top of the *frictionless datapackage*
specification.

.. [#] We use the term "data format" to refer to the general structure
       of the data, and its metadata, instead of a specific file type
       like *CSV*, *Excel*, etc.


What is a ``datapackage``?
++++++++++++++++++++++++++

A datapackage consists of a set of data files, any related source
code, relevant licenses, and the ``datapackage.json`` file, that
records all this information in a single place.  It is based on a
widely recognised standard called *frictionless data*; further detals
of the specification can be found on their website_.  This file also
includes specific information about the structure of each dataset (or
*data resource*).  A data resource can be any kind of file, like
*CSV*, *Excel*, etc.  At the moment, only tabular resources are
supported.  Each data resource has an entry which states its name,
relative path, and structure (or *schema*) of the data within it.
Structural information includes column names, the type of data stored
in each column, instructions on how to identify missing values, or how
to uniquely identify each row in a dataset (otherwise known as
"primary key").  Since data can often be large, it is possible to
split the schema into a separate file and include it from
``datapackage.json`` so as to keep it manageable and easy to work
with.

We provide a web-based user interface (web UI) and a Python API to
create datasets.  The web UI is meant to be easy to use, and requires
no programming knowledge, whereas the Python API is more fully
featured and requires some understanding of the scientific Python
ecosystem.  At the moment there is no direct support for other
languages, but the frictionless data specification provides some
alternate implementations of the datapackage format which maybe used.
However, since the underlying design uses commonly used facilities,
e.g. using *JSON* for metadata, and relying on well established file
formats; adding support in other languages is a matter of allocating
resources.

.. _website: https://specs.frictionlessdata.io/
