Learning by example
-------------------

Now that we understand the general design of the registry, lets take a
look at an example dataset, and consider how we can use the registry
to describe the schema.  Say we have the following dataset:

.. tabularcolumns:: |l|c|r|
.. csv-table:: Capacity Factor
   :file: capacity_factor.csv
   :header-rows: 1

Our value column, capacity factor, is a kind of efficiency:

.. math::

   capacity\_factor ̱ ∈  [0, 1]

So the corresponding entry in the registry would
be::

  {
    "name": "capacity_factor",
    "type": "number",
    "format": "default",
    "constraints": {
        "minimum": 0,
        "maximum": 1
    }
  }

The first column, ``technology``, should have an entry like this::

  {
    "name": "technology",
    "type": "string",
    "format": "default",
    "constraints": {
	"enum": [
	    "ccgt",
	    "free-transmission"
	]
    }
  }

The enum property signifies that the column can only have values
present in this set.  The ``timestep`` column looks like this::

  {
    "name": "timesteps",
    "type": "datetime",
    "format": "default"
  }

To describe our dataset, we need to mark the columns ``technology``
and ``timestep`` as index-columns.  So the final schema would look
like this::

  {
    "fields": [
      {
        "name": "technology",
        "type": "string",
        "format": "default",
        "constraints": {
          "enum": [
              "ac_transmission",
              "ccgt",
          ]
        }
      },
      {
        "name": "timestep",
        "type": "datetime",
        "format": "default"
      },
      {
        "name": "capacity_factor",
        "type": "number",
        "format": "default",
        "constraints": {
          "minimum": 0,
          "maximum": 1
        }
      }
    ],
    "missingValues": [
      "null"
    ],
    "primaryKey": [
      "technology",
      "timestep"
    ]
  }

where, the key ``primaryKey`` indicates the set of index columns.

.. _index-tutorial:

Introducing the index file
++++++++++++++++++++++++++

The implementation of *Friendly data* simplifies the above process by
introducing an "index" file.  In essence, much like the index of a
book, it is the "index" of a data package.  An index file lists
datasets within the data package, and identifies columns in each
dataset that are to be treated as the primary key (or index).
Sometimes an index file entry may also contain other related info.

Let us examine how that might look using the capacity factor dataset
as an example.  The corresponding entry would look like this::

  - path: capacity_factor.csv
    idxcols:
    - technology
    - timestep

It can be stored in the ``index.yaml`` file in the top level directory
of the package.  An index also supports attaching other information
about a dataset, e.g. if you need to skip ``n`` lines from the top
when reading the corresponding file, you can simply add the key
``skip: n``.  All datasets need not be included in the index, just
that if a dataset is not included, it does not gain from the
structured metadata already recorded in the *Friendly data registry*,
but is otherwise perfectly valid; more details about the index file
can be found at :ref:`index-file`.
