Metadata
--------

One of the advantages of using a data package is to be able to attach
detailed metadata to your datasets.  The CLI interface makes this
relatively straightforward.  You can create a dataset by calling::

  $ friendly_data create --name my-pkg --licenses CC0-1.0 \
        --keywords 'mymodel renewables energy' \
	path/to/pkgdir/index.yaml path/to/pkgdir/data/*.csv \
	--export my-data-pkg

Some of the options are mandatory, like ``--name`` and ``--licenses``.
The command above will create a dataset with all the ``CSV`` files
that match the command line pattern, and all the files mentioned in
the index file.  All datasets are copied to ``my-data-pkg``, and
dataset schemas and metadata is written to
``my-data-pkg/datapackage.json``.  If instead of ``--export``,
``--inplace`` (without arguments) is used, the datasets are left in
place, and the metadata is written to
``path/to/pkgdir/datapackage.json``.


A more convenient way to specify the metadata would be to use a
configuration file, like this::

  $ friendly_data create --metadata conf.yaml \
        pkgdir/index.yaml pkgdir/data/*.csv \
	--export my-data-pkg

The configuration file looks like this::

  metadata:
    name: mypkg
    licenses: CC0-1.0
    keywords:
      - mymodel
      - renewables
      - energy
    description: |
      This is a test, let's see how this goes.

If there are multiple licenses, you can provide a list of licenses
instead::

  metadata:
    licenses: [CC0-1.0, Apache-2.0]

Updating existing packages
++++++++++++++++++++++++++

You can also modify existing data packages like this::

  $ friendly_data update --licenses Apache-2.0 path/to/pkg 

Here you can see the package is being relicensed under the Apache
version 2.0 license.  You could also add a new dataset like this::

  $ friendly_data update path/to/pkg path/to/new_*.csv

You could also update the metadata of an existing dataset by updating
the index file before executing the command.  You can find more
documentation about the CLI by using the ``--help`` flag.

::

   $ friendly_data --help
   $ friendly_data update --help
