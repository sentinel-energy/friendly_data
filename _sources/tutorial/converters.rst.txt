Reading into ``pandas`` or ``xarray``
-------------------------------------

A datapackage can be read into Python by using the ``frictionless``
library.  However, ``friendly_data`` provides alternative more direct and
performant converters for popular data analysis libraries like
``pandas``, ``xarray``, etc.

::

    from friendly_data.converters import to_df

    pkg = ... # read datapackage from disk

    # read all data resources as a dataframe
    dfs = [to_df(resource) for resource in pkg["resources"]]

For ``xarray.DataArray`` and ``xarray.Dataset`` you can use ``to_da``
and ``to_dst``.  There is also ``to_mfdst`` that provides an API to
read an entire package into a multi-file dataset::

  mfdst = to_mfdst(pkg["resources"])

A column (or field) is converted to a ``pandas`` type as per the
following mapping:

============  ===============
schema type   ``pandas`` type
============  ===============
``boolean``   ``bool``
``datetime``  ``datetime64``
``integer``   ``Int64``
``number``    ``float``
``string``    ``string``
============  ===============

Note the choice of ``Int64`` and ``string`` among the types.  This
adds a minimum requirement of Pandas version 1.0.  ``Int64`` allows
you to have missing values in integer columns, which would be
otherwise impossible; and using ``string`` is much more space
efficient, making it easier to manage larger datasets with lots of
text fields.

The functions also respect other information in the schema like custom
missing values, primary keys, etc.
