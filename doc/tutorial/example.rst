Learning by example
-------------------

Now that we understand the general design of the registry, lets take a
look at an example dataset, and consider how we can use the registry
to describe schema.  Say we have the following dataset:

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

The first columns: ``techs``, has an entry like this::

  {
    "name": "techs",
    "type": "string",
    "format": "default",
    "constraints": {
	"enum": [
	    "ccgt",
	    "free-transmission"
	]
    }
  }

The enum signifies that the calumn can only have values present in
this set.  The ``timesteps`` column looks like this::

  {
    "name": "timesteps",
    "type": "datetime",
    "format": "default"
  }

To describe our dataset, we need to mark the columns ``techs`` and
``timesteps`` as index-columns.  So the final schema would look like
this::

  {
    "fields": [
      {
        "name": "techs",
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
        "name": "timesteps",
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
      "techs",
      "timesteps"
    ]
  }

where, the key ``primaryKey`` indicates the set of index columns.

.. _index-tutorial:

Introducing the index file
++++++++++++++++++++++++++

The *SENTINEL archive* implementation simplifies the process by
introducing an "index" file.  In essence, much like the index of a
book, it is the "index" of a data package.  An index file lists
datasets within the data package, and identifies columns in each
dataset that are to be treated as the primary key (or index).
Sometimes an index file entry may also contain other related info.

Let us examine how that might look using the capacity factor dataset
as an example.  The corresponding entry would look like this::

  - path: capacity_factor.csv
    idxcols:
    - techs
    - timesteps

It can be stored in a file called ``index.yaml`` in the top level
directory of our package.  You can also use the ``JSON`` syntax
instead of ``YAML``, it will have identical results, and the file
should be called ``index.json``.  An index also supports attaching
other information about a dataset, like if you need to skip ``n``
lines from the top when reading the corresponding file, you can simply
add the key ``skip: n``.  All datasets need not be included in the
index, just that if a dataset is not included, it does not gain from
the structured metadata already recorded in the *SENTINEL archive
registry*, but otherwise perfectly valid; more details can be found at
:ref:`index-file`.

In the following sections we will go through the software tools
provided by the project and how to best leverage it.
