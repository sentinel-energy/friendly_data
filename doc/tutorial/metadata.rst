Metadata
--------

One of the advantages of using a data package is to be able to attach
a significant amount of metadata to your datasets.  The CLI interface
makes this relatively straightforward.  You can create a dataset by
calling::

  $ sentinel-archive create --name my-pkg --license CC0-1.0 \
        --keywords 'mymodel renewables energy' \
	path/to/pkgdir/index.yaml path/to/pkgdir/data/*.csv

There are other options as well, and some of them are mandatory, like
``--name`` or ``--license``.  The command above will create a dataset
with all the ``CSV`` files that match the command line glob.  If there
are corresponding entries in the index file, it will be used to enrich
the generic structure information (schema) that is inferred by
sampling the data in the datasets.  For datasets without an entry in
the index, the inferred schema is retained.  The dataset schemas, and
metadata is stored in a ``JSON`` file:
``path/to/pkgdir/datapackage.json``.  You can also use a configuration
file to provide the metadata like this::

  $ sentinel-archive create --metadata conf.yaml \
        pkgdir/index.yaml pkgdir/data/*.csv

The configuration file could look like this::

  name: mypkg
  license: CC0-1.0
  keywords:
    - mymodel
    - renewables
    - energy
  description: |
    This is a test, let's see how this goes.

You can of course use ``YAML`` and ``JSON`` files interchangeably
except for the ``datapackage.json``.

You can do everything mentioned above and much more, using the Python
API. e.g.::

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

Consult the API documentation for more details.

Updating existing packages
++++++++++++++++++++++++++

You can also modify existing data packages using something like::

  $ sentinel-archive update --license Apache-2.0 path/to/pkg 

Here you can see the package is being relicensed under the Apache
version 2.0 license.  You could also add a new dataset, or update an
existing dataset by updating the index file before executing the
command.  You can find more documentation about the CLI by using the
``--help`` flag.

::

   $ sentinel-archive --help
   $ sentinel-archive update --help

When using the Python API, there is complete freedom on how you want
to update the schema.  To illustrate with an example, say we have a
time series dataset called ``electricity-consumption``, and the first
column in the file contains timestamps, or ``datetime`` values.
However the time format is non-standar.  To explicitly specify this,
we can use the snippet below::

    from sark.dpkg import update_pkg

    update_fields = {
        "time": {"name": "time", "type": "datetime", "format": "%b %d, %Y"},
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

If for instance there is a need to add to the default list of missing
values, or to update the set of fields of primary keys, you could use
the following::

    update = {
        "primaryKey": ["lvl", "TRE", "IUY"],
        "missingValues": ["", "nodata"]
    }
    success = update_pkg(pkg, "electricity-consumption"), update, fields=False)

In the above example, the fields: "lvl", "TRE", and "IUY", are
specified as primary keys (used to uniquely identify a row in a
dataset).  Similarly, the token ``nodata`` is being added to the list
of values to be treated as a missing value.
