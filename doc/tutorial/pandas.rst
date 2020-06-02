Reading as a ``pandas.DataFrame``
---------------------------------

A datapackage can be read into Python by using the underlying
``datapackage`` library.  However it is not very performant, and does
not integrate with the popular data analysis library ``pandas``.  That
is why the SENTINEL archive library provides a helper function that
reads the package schema, and reads a data resource directly into a
``pandas.DataFrame``. ::

    from sark.dpkg import to_df

    pkg = ... # read datapackage from disk

    # read all data resources as a dataframe
    dfs = [to_df(resource) for resource in pkg.resources]

Depending on the type specified in the schema, a field is converted to
a ``pandas`` type.  The mapping between the two kinds of types are
summarised below:

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
