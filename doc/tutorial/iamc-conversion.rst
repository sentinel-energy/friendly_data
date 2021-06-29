Converting to IAMC format
-------------------------

The IAMC format from the IAM consortium is popular in the energy
modelling community.  So Friendly data provides workflows to convert a
data package to IAMC output with some configuration.

The IAMC format allows the user to define their own hierarchy of
variables.  So when using Friendly data, you can associate specific
files to different branches of the hierarchy.  There are currently
two ways of specifying this:
1. use a fixed string, and
2. use a format string with one or more user defined index columns.

A format string can be specified in the index file by adding an `iamc`
key.  If the string contains a column name enclosed in braces, when
creating the IAMC file, corresponding values from the index column
will be substituted in that position.

Let us consider the `example data package`_::

  $ tree 
  .
  ├── carrier.csv
  ├── conf.yaml
  ├── data
  │   ├── flow_in_25.csv
  │   ├── flow_in_50.csv
  │   ├── flow_in_sum.csv
  .   .
  .   .
  │   ├── flow_out_sum_const1.csv
  │   ├── flow_out_sum_multi.csv
  .   .
  .   .
  ├── datapackage.json
  ├── index.yaml
  ├── LICENSE
  ├── README.md
  └── technology.csv

If we consider the dataset `flow_in_sum.csv`, which looks like:

.. csv-table:: Energy flow in
   :file: flow_in_sum.csv
   :header-rows: 1

The IAMC format requires that the data have the columns: `model`,
`scenario`, `region`, `variable`, `unit`, and `value`.  If the data is
in "long format", then it should also have a column `year`.  In the
above dataset, `locs` is an alias for `region`, but there are no
columns for `model`, `variable`, or `value`, and there is an
additional column called `techs`.

The corresponding entry in the index file looks something like this::

  - alias:
      locs: region
      techs: technology
    iamc: Hourly power consumption|Yearly|{technology}
    idxcols:
    - techs
    - scenario
    - year
    - locs
    - unit
    name: Hourly power consumption|Yearly
    path: data/flow_in_sum.csv

The `alias` key declares that, `techs` is to be treated as
`technology`, and `locs` as `region` - that satisfies one of the
missing columns required by the IAMC specification.  You will also
note, there is a `iamc` key.  This mentions `technology` in ``{...}``.
This is a format string, which means all occurences of `technology`
are to be replaced by the corresponding values in data; in this case
`electric_heater` and `light_transport_ev`.  The resulting string is
then available under the `variable` column.  However you will note,
the technology names are not particularly descriptive, so you probably
want to replace them with something more commonly used in an IAMC
dataset.  These alternate names can be specified in a separate CSV
file, and provided in the configuration file.  If we refer to the
`example data pacakge`_, we will find a ``conf.yaml`` file, which has
a section like this::

  indices:
    technology: technology.csv
    carrier: carrier.csv
    model: calliope

The above configures technology names to be resolved as per
``technology.csv``, which looks like this:
    
.. csv-table:: Technology definitions
   :file: technology.csv
   :header-rows: 1

In the same configuration snippet, you can see there's a key for
`model`, but instead of pointing to a file like `technology`, it
specifies a string.  If a `model` column does not exist in your
dataset, this string will be taken as the default value for such a
column.  This leaves only the `value` column, which is nothing but the
data column, in our example, `flow_in_sum`.  And we have our data in
IAMC format!

.. csv-table:: Data in IAMC format
   :file: iamc.csv
   :header-rows: 1

This kind of replacement
from values in the dataset can de done with multiple columns, e.g. the
index entry for `flow_out_25_multi.csv` looks like this::

  - iamc: Generation|Percentile 25|{carrier}|{technology}
    idxcols:
    - carrier
    - technology
    - scenario
    - year
    - region
    - unit
    name: Generation|Percentile 25
    path: data/flow_out_25_multi.csv

Here, all possible combinations of `technology` and `carrier` will be
tried, and only the ones present in the data will be included in the
final output.  If you do not need something like this, you can always
use a regular string (without any ``{...}``) to denote what should be
in the `variable` column (see the `example data package`_ for other
examples).


.. _`example data package`: https://github.com/sentinel-energy/friendly_data_example
