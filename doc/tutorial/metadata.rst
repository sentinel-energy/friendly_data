Metadata
========

To create a ``datapackage.json`` file, you need not write it from
scratch.  A datapackage can be created within Python with the
following::

    from sark.dpkg import create_pkg
    from sark.metatools import get_license

    pkg_meta = {
        "name": "dataset_for_xyz",
        "title": "Dataset for XYZ",
        "description": "This dataset is for XYZ and spans 10 years",
        "licenses": [get_license("CC0-1.0")],
	"keywords": ["XYZ", "ABC"],
    }
    pkg = create_pkg(pkg_meta, Path("data").glob("*.csv"))

In the above snippet the dictionary ``pkg_meta`` sets the metadata,
whereas all *CSV* files in the subdirectory ``data/`` are added to the
datapackage as data resources.  As the files are read, a basic
*schema* is guessed.  You may inspect the contents of the datapackage
by looking at ``pkg.descriptor``.

Schema
======

The schema can be specialised further by updating the metadata for
each field.  To illustrate with an example, say we have a time series
dataset called "electricity-consumption", and the first column in the
file contains timestamps, or ``datetime`` values.  The heuristics that
infers the schema detects the column as a ``string`` (plain text).  To
rectify this, we can use the snippet below::

    from sark.dpkg import update_pkg

    update_fields = {
        "time": {"name": "time", "type": "datetime", "format": "default"},
    }
    success = update_pkg(pkg, "electricity-consumption"), update_fields)

    if success:
        print(f"successfully updated: {list(update_fields)}")
    else:
        print(f"failed to update: {list(update_fields)}")

If multiple fields need updating, you only need to add a key
corresponding to the field in the ``update_fields`` dictionary; the
following snippet also updates the "QWE" column such that it is
interpreted as ``numbers``::

    update_fields = {
        "time": {"name": "time", "type": "datetime", "format": "default"},
	"QWE": {"name": "QWE", "type": "integer", "format": "default"},
    }

Missing values & primary keys
+++++++++++++++++++++++++++++

If for instance there is a need to add to the default list of values
that are treated as missing values, or to specify a set of fields as
primary keys, use the following::

    update = {
        "primaryKey": ["lvl", "TRE", "IUY"],
        "missingValues": ["", "nodata"]
    }
    success = update_pkg(pkg, "electricity-consumption"), update, fields=False)

In the above example, the fields: "lvl", "TRE", and "IUY", are
specified as primary keys (used to uniquely identify a row in a
dataset).  Similarly, the token ``nodata`` is being added to the list
of values to be treated as a missing value.
