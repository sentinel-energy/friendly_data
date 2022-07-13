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

**name** (string)

    Typically the name of a dataset is derived from its file name, but
    when working with the Python API, this key is used to map a
    dataframe to an entry in the index file.  This can also be used to
    map a table in a database to an entry (where the ``path`` key
    points to the database, e.g. path to an sqlite file).

**alias** (mapping or dictionary)

    A mapping of column names in the dataset that should be mapped to
    another column in the registry; say you use ``node`` for
    locations, and you want the corresponding column to be mapped to
    ``region`` in the registry.  This can be specified with an index
    entry like this:

    .. code-block:: yaml

      - path: demand.csv
        idxcols: [node, timestep]
        alias: {node: region}

**iamc** (string)

    A format string to construct the IAMC variable for a file entry.
    It can reference index columns by enclosing them in braces (like a
    Python format string)::

      Installed Capacity|{carrier}|{technology}

**agg** (mapping or dictionary)

    A mapping of index column name to a list of aggregation rules (for
    IAMC conversion) which is another mapping of the form:

    .. code-block:: yaml

      values:
      - open_field_pv
      - roof_mounted_pv
      variable: Primary Energy|Solar

    As there can be multiple rules for a column, they are included as
    a list.  A complete index entry with aggregation rules looks like:

    .. code-block:: yaml

      - agg:
        technology:
        - values:
          - dac
          variable: Carbon Sequestration|Direct Air Capture
        - values:
          - hydro_reservoir
          - hydro_run_of_river
          variable: Primary Energy|Hydro
        - values:
          - open_field_pv
          - roof_mounted_pv
          variable: Primary Energy|Solar
      iamc: Primary Energy|{technology}
      idxcols:
      - carrier
      - technology
      - year
      path: flow_out_sum.csv

    With the above entry, when converting to IAMC format, all data
    points with technology ``open_field_pv`` and ``roof_mounted_pv``
    will be added together under the IAMC variable name ``Primary
    Energy|Solar``.  Note that multiple index columns cannot be
    combined in this manner; only one is possible.

.. [#] It is similar to index of a book, which allows you to jump to a
       specific page in the book by looking up a keyword.
