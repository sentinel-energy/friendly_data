.. _registry:

Data registry
=============

Friendly data adds functionality specific to the energy modelling
community via the registry.  It is a collection of metadata for
commonly used data columns.  It each column must have a generic name,
and a data type (a complete list of all supported data types can found
in the `frictionless documentation`_).  Additionally the registry also
records metadata like ``constraints``.  Some constraints, like
``enum``, where you limit the allowed values in a column to a set,
depends on the dataset.  These are mentioned in the registry, but the
value is left blank, and is determined from the dataset at runtime.

Using a package index file we can further use the metadata and specify
index columns, aliases, etc.

.. toctree::
   :maxdepth: 1

   api/registry
   api/index-file

.. api/registry is auto-generated during a CI run

.. _`frictionless documentation`: https://specs.frictionlessdata.io/table-schema/
