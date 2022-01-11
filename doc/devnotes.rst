Notes for developers
====================

Adding dependencies
-------------------

If the dependency is minor, i.e.

- used in only a few places, say restricted to a single module, or

- does not need to be documented, e.g. not visible as type hints, for
  function arguments or return type,

import statements should be hidden inside a function (see
``friendly_data.cli`` for examples):

.. code-block:: python

   def my_func():
       from dependency import feature
       # use `feature`

If you really have to add a dependency,

- update the list of mocked modules in ``doc/conf.py``, and

- may need to update patch to ``requirements*.txt`` for CI:
  ``dev/requirements*.patch``.

Working with the data package API
---------------------------------

- workaround for POSIX compliant paths on Windows, filter paths
  through ``friendly_data.dpkg._ensure_posix``

- metadata extensions:

  - always use ``alias``-aware functions like
    :func:`friendly_data.dpkg.get_aliased_cols`

- fill index levels (``enum`` values) using
  :func:`friendly_data.dpkg.index_levels`. note that this reads the
  resource using ``pandas``, so there is a performance cost to each
  call

Working with the index file
---------------------------

The index file represents as a list of records for each dataset/file
in the data package.  This should be always read using the
:class:`friendly_data.dpkg.pkgindex`.  The class also provides methods
to conveniently iterate over records with certain guarantees (see the
API docs).


Working with the registry API
-----------------------------

- If you want to work with the registry that is published on GH/PyPI,
  you should use the functions provided by ``friendly_data_registry``.

- If you want to work with a registry that is user customisable (from
  a config file, or a dictionary), use the context manager and wrapper
  functions provided by ``friendly_data.registry``.

Converters: interfacing with the Python ecosystem
-------------------------------------------------

All converters internally use the pandas converter for dataframes.  If
additional converters are added, they should continue to follow this
convention.  This is prefered as it isolates the implementation of
custom extensions to the frictionless specification (like the
``alias`` functionality) to a limited number of functions:
:func:`friendly_data.converters.to_df` and
:func:`friendly_data.converters.from_df`.
