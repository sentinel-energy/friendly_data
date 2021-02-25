Understanding the SENTINEL archive registry
-------------------------------------------

Currently only tabular datasets are supported, so we will limit our
discussion to tabular datasets like tables with one or more columns
(including time series).  When working with tables it is common to
agree on a convention on how to identify rows uniquely.  Typically
this is done by designating a column (or a set of columns) as the
index (or primary key).  And like the index in a book, index entries
are required to be unique.  We follow the same principle here, and
categorise a column either as an index column, or a regular (value)
column.  Within the ``sark`` framework we refer to them as ``idxcols``
and ``cols`` respectively.

.. figure:: _static/images/registry-flow-create.png
   :width: 90%
   :align: center

   Schematic representation of a data package, composed of multiple
   datasets, where metadata for various columns are taken from the
   registry.  *Note*: the last column in the last dataset is not
   present in the registry, denoting that there is flexibility to
   deviate from the accepted consensus when necessary.

While a table (dataset/data resource) in a data package can have any
number of columns of either kind, it is often helpful during analysis
to designate an index.  ``sark`` implements this by having an external
registry that records all columns that are generally useful in the
context of SENTINEL models, and categorising these columns as one or
the other.  These could be something like ``capacity_factor`` or a cap
on energy storage costs (``cost_storage_cap``), or coordinates of a
site or location (``loc_coordinates``), or something much more generic
like ``timesteps`` indicating the timesteps of a demand profile.
Among the aforementioned columns, ``timesteps`` is the only
index-column.

.. figure:: _static/images/registry-flow-update.png
   :width: 90%
   :align: center

   Updates to the registry undergoes a review process to gain
   consensus in the community.  This should limit duplication of
   effort, and over time formalise the terminology.

In the beginning the registry will be evolving with time, and proposal
for inclusion of new columns to suit your models, or renaming existing
columns, or any other relavant changes are welcome.  The goal is to
reach a consensus as to what conventions suit most of the SENTINEL
partners the best.

Besides naming and classifying columns, the registry also has type
information; e.g. ``timesteps`` is of type ``datetime`` (timestamp
with date), GPS coordinates are pairs of ``loc_coordinates``, so it is
a fractional number (``number``), ``techs`` on the other hand are
names of technologies, so they are strings.  It can also include
constraints, e.g. ``capacity_factor`` is a ``number`` between ``0``
and ``1``, or ``techs`` can take one of a set of predefined values.
Now you might notice that, while everyone will agree with the
constraint on ``capacity_factor``, the constraint on ``techs`` will be
different for different models.  So this element is configurable, and
the ``sark`` implementation infers the valid set by sampling the
dataset during package creation.

To review the current set of columns in the registry, please consult
the complete registry :ref:`documentation <registry>`.  Any changes or
additions can be suggested by opening a Pull Request (PR) in the
`SENTINEL archive registry repository`_ on GitHub.

.. _`SENTINEL archive registry repository`:
   https://github.com/sentinel-energy/sentinel-archive-registry

Column schema
+++++++++++++

The column schema can be specified either in JSON or YAML format.  The
general structure is a ``Mapping`` (set of key-value pairs)::

  {
    "name": "energy_eff",
    "type": "number",
    "format": "default",
    "constraints": {
        "minimum": 0,
        "maximum": 1
    }
  }

while only the ``name`` property is mandatory in the *frictionless*
specification, for SENTINEL archive we also expect the ``type``
property.  Constraints on the field can be specified by providing the
``constraints`` key.  It can take values like ``required``,
``maximum``, ``minimum``, ``enum``, etc; see the frictionless
documentation_ for details.

.. _documentation:
   https://specs.frictionlessdata.io/table-schema/#types-and-formats
