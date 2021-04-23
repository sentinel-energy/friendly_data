.. _index-file:

Keys supported by the Package Index file
----------------------------------------

**path** (string)

    Relative path to a dataset

**idxcols** (list of strings)

    Column names that should be considered part of the index of a dataset

**skip** (positive integer)

    Number of lines to skip when reading the dataset

**name** (string) *currently unused*

    Typically the name of a dataset is derived from its file name, but
    in the future this key might be used to provide an alternate more
    descriptive name to a dataset.

**alias** (mapping or dictionary)

    A mapping of column names in the dataset that should be mapped to
    another column in the registry; e.g. you use ``node`` for
    locations, and you want the corresponding column to be mapped to
    ``region`` in the registry.  This can be specified with an index
    entry like this::

      - path: demand.csv
        idxcols: [node, timestep]
        alias: {node: region}
