Converting to IAMC format
-------------------------

The IAMC format from the IAM consortium is popular in the energy
modelling community.  So Friendly data provides workflows to convert a
data package to IAMC output with some configuration.

The IAMC format allows the user to define their own hierarchy of
variables.  So when using Friendly data, you can associate specific
files to different branches of the hierarchy.  There are currently
three ways of specifying this:
1. use a fixed string,
2. use a format string with one or more user defined index columns, and
3. define a set of values that are combined and mapped to an IAMC variable.

A format string is specified in the index file by adding an ``iamc``
key.  If the string contains a column name enclosed in braces, when
creating the IAMC file, corresponding values from the index column
will be substituted in that position.

Let us consider the `example data package`_::

  $ tree 
  .
  ├── annual_cost_per_nameplate_capacity.csv
  ├── carrier.csv
  ├── conf.yaml
  ├── datapackage.json
  ├── emissions_per_flow_in.csv
  ├── flow_out_sum.csv
  ├── index.yaml
  ├── LICENSE
  ├── nameplate_capacity.csv
  ├── README.md
  └── technology.csv

If we consider the dataset ``flow_out_sum.csv``, which looks like:

.. csv-table:: Energy flow out
   :file: _static/data/flow_out_sum.csv
   :header-rows: 1

The IAMC format requires that the data have the columns: ``model``,
``scenario``, ``region``, ``variable``, ``unit``, and ``value``.  If
the data is in "long format", then it should also have a column
``year``.  In the above dataset, ``locs`` is an alias for ``region``,
but there are no columns for ``model``, ``variable``, or ``value``,
and there is an additional column called ``techs``.

The corresponding entry in the index file looks something like this::

  - agg:
      technology:
      - values:
        - wind_onshore
        - wind_offshore
        variable: Primary Energy|Wind
    alias:
      locs: region
      techs: technology
      carriers: carrier
    iamc: Primary Energy|{technology}
    idxcols:
    - scenario
    - carriers
    - techs
    - locs
    - unit
    - year
    path: flow_out_sum.csv

The ``alias`` key declares that, ``techs`` is to be treated as
``technology``, and ``locs`` as ``region`` - that satisfies one of the
missing columns required by the IAMC specification.  You will also
note, there is a ``iamc`` key.  This mentions ``technology`` in
``{...}``.  This is a format string, which means all occurences of
``technology`` are to be replaced by the corresponding values in data.
The ``agg`` key also specifies a rule that combines two technologies
under a single name.  The dataset has ``wind_onshore``,
``wind_offshore``, and ``nuclear``.  While ``wind_*`` technologies are
summed together, ``nuclear`` is replaced in the format string to form
the IAMC variable.  The resulting strings are then available under the
``variable`` column.  However you will note, the technology names are
not particularly descriptive, so you probably want to replace them
with something more commonly used in an IAMC dataset.  These alternate
names can be specified in a separate CSV file, and provided in the
configuration file.  If we refer to the `example data package`_, we
will find a ``conf.yaml`` file, which has a section like this::

  indices:
    technology: technology.csv
    carrier: carrier.csv
    model: calliope

The above configures technology names to be resolved as per
``technology.csv``, which looks like this:
    
.. csv-table:: Technology definitions
   :file: _static/data/technology.csv
   :header-rows: 1

In the same configuration snippet, you can see there's a key for
``model``, but instead of pointing to a file like ``technology``, it
specifies a string.  If a ``model`` column does not exist in your
dataset, this string will be taken as the default value for such a
column.  This leaves only the ``value`` column, which is nothing but
the data column, in our example that is ``flow_out_sum``.  And we have
our data in IAMC format!

.. csv-table:: Data in IAMC format
   :file: _static/data/iamc.csv
   :header-rows: 1

This kind of replacement
from values in the dataset can de done with multiple columns, e.g. the
index entry for ``nameplate_capacity.csv`` looks like this::

  - agg:
      technology:
      - values:
        - wind_onshore
        - wind_offshore
        variable: Capacity|Electricity|Wind
    alias:
      locs: region
      techs: technology
      carriers: carrier
    iamc: Capacity|{carrier}|{technology}
    idxcols:
    - scenario
    - carriers
    - techs
    - locs
    - unit
    - year
    path: nameplate_capacity.csv

Here, all possible combinations of ``technology`` and ``carrier`` will
be tried, and only the ones present in the data will be included in
the final output.  If you do not need replacement from data, you can
always use a regular string (without any ``{...}``) to denote what
should be in the ``variable`` column (see the `example data package`_
for other examples).


.. _`example data package`: https://github.com/sentinel-energy/friendly_data_example
