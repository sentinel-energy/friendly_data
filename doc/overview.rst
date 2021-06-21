Overview
========

SENTINEL partners work with a variety of models in diverse computing
environments.  The *Friendly data* format [#]_ has been designed to
facilitate interoperability in this diverse ecosystem by prioritising
ease of sharing.  Since we have to cope with a variety of research
requirements, and workflows, the format is fairly flexible, and to a
great extent self-descriptive.  As the author of a model or dataset is
the most knowledgeable person about their work, we rely on the author
to describe it accurately, and provide tools to simplify the process.
A typical description of a dataset includes metadata and structural
information.

**Metadata** of a dataset typically establishes the context for the
dataset.  It can consist of properties like:

- a computer program friendly name so that they can be referred to
  easily from software,
- a title and free-form description so that other researchers using
  the dataset are aware of its provenance and can use it correctly,
- search keywords for easier discoverability on online platforms,
- license information so that others know the terms of use, and
- citation information.

Whereas **structural information** should state the type information
of a dataset, specify any constraints, and assumptions implicit in the
data.  To avoid duplication of effort, our implementation builds on
top of the *frictionless datapackage* specification_.

.. [#] We use the term "data format" to refer to the general structure
       of the data and its metadata, instead of a specific file type
       like *CSV*, *Excel*, etc.

.. _specification: https://frictionlessdata.io/

What is a data package?
++++++++++++++++++++++++++

A data package consists of a set of data files, any related source
code, relevant licenses, and a ``datapackage.json`` file that records
all this information in a single place.  It is based on a widely
recognised standard called *frictionless data*; further details of the
specification can be found on their `online documentation`_.  This
file also includes specific information about the structure of each
dataset (or *resource*) included in the datapackage.  A resource can
be any kind of file, like *CSV*, *Excel*, etc.  At the moment, only
tabular resources are supported (you can still add other kinds of
files, just that the framework won't be able to read them or do
anything meaningful with it).  Each data resource has an entry which
states its name, relative path, and structure (or *schema*) of the
data within it.  Structural information includes column names, the
type of data stored in each column (number, integer, string, etc),
instructions on how to identify missing values, or how to uniquely
identify each row in a dataset (otherwise known as the "primary key").

There is a `web-based user interface`_ (web UI) provided by the
original developers of the *frictionless data* specification.  While
it can be used for smaller or simpler data packages, it does not
support any of the SENTINEL specific additions.  The web UI is meant
to be easy to use, and requires no knowledge of programming.

The Friendly data library conforms to this specification, however it
adds a few other SENTINEL specific conventions designed to facilitate
interoperation between various models.  You can either use the command
line interface (CLI) or the Python API to create or manage data
packages.  While the underlying frictionless data specification
provides alternate implementations of the datapackage format in other
programming languages, at the moment Friendly data is only available
in Python.  However, since the underlying design uses established file
formats, e.g. using *JSON* for metadata, and *CSV* for dataset; there
is no barrier to reading a data package using other languages.

.. _`online documentation`: https://specs.frictionlessdata.io/
.. _`web-based user interface`: https://create.frictionlessdata.io/
