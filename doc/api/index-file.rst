.. _index-file:

The Package Index file
----------------------

Column values that are unique across a dataset can be used to identify
a specific row.  These columns are referred to as index columns [#]_,
alternatively they are also referred to as the *primary key*.  Using a
*package index file* we can combine column metadata and specify these
index columns, column aliases, etc.  The set of keys that can be used
in a package index file is documented below.

**path** (string)

    Relative path to a dataset

**idxcols** (list of strings)

    Column names that should be considered part of the index of a
    dataset (or primary key)

**skip** (positive integer)

    Number of lines to skip when reading the dataset

**name** (string) *currently unused*

    Typically the name of a dataset is derived from its file name, but
    in the future this key might be used to provide an alternate more
    descriptive name to a dataset.

**alias** (mapping or dictionary)

    A mapping of column names in the dataset that should be mapped to
    another column in the registry; say you use ``node`` for
    locations, and you want the corresponding column to be mapped to
    ``region`` in the registry.  This can be specified with an index
    entry like this::

      - path: demand.csv
        idxcols: [node, timestep]
        alias: {node: region}

**iamc** (string)

    A format string to construct the IAMC variable for a file entry.
    It can reference index columns by enclosing them in braces (like a
    Python format string)::

      Installed Capacity|{carrier}|{technology}


.. [#] It is similar to index of a book, which allows you to jump to a
       specific page in the book by looking up a keyword.
