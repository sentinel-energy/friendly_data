The ``friendly_data`` tool
--------------------------

The Friendly data command line (CLI) tool can be used to create and
manage data packages.  It relies on two files to generate the metadata
correctly.  The first is an YAML configuration file with metadata like
name, title, licenses, etc::

  metadata:
    name: foo-bar-baz
    title: Foo Bar Baz
    description: This is a test data package
    keywords:
      - foo
      - bar
    licenses: [CC0-1.0, Apache-2.0]

As mentioned in :ref:`index-file`, you can provide a package index
file like below to specify the index columns in a dataset::

  - path: nameplate_capacity.csv
    idxcols:
    - scenario
    - region
    - technology
  - path: resource.csv
    idxcols:
    - scenario
    - region
    - technology

In the above example, the columns *scenario*, *region*, and
*technology* are index columns; all other columns in the dataset will
be treated as value columns.

So to create a data package with the following datasets:

.. csv-table:: Nameplate capacity
   :file: _static/data/nameplate_capacity.csv
   :header-rows: 1

.. csv-table:: Resource
   :file: _static/data/resource.csv
   :header-rows: 1

We use the CLI like this::

  $ friendly_data create index.yaml --metadata config.yaml --export output/
  friendly_data.metatools: WARNING: inappropriate license: not data
  Package metadata: output/datapackage.json
  $ friendly_data describe output/
  name: foo-bar-baz
  title: Foo Bar Baz

  description:
  This is a test data package

  keywords: foo, bar
  licenses: CC0-1.0, Apache-2.0

  resources:
  ---
  path: nameplate_capacity.csv
  fields: scenario, region, technology, nameplate_capacity
  ---
  path: resource.csv
  fields: scenario, region, technology, resource
  ---

To see all available options, just pass the ``--help`` flag to the
command::

  $ friendly_data --help
  $ friendly_data <command> --help
