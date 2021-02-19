Persistence
-----------

The CLI always writes out the finalised data package as
``datapackage.json`` to the package directory.  When using the Python
API however, you have to do that yourself.  Note that the path
references in the package metadata are relative to the package
directory, so the ``JSON`` file needs to be in the top directory of
the package.  From Python you can do something like this::

    from sark.dpkg import write_pkg

    write_pkg(pkg, "path/to/pkgdir")

You can also write an updated index - which is nothing other than a
``pandas.DataFrame`` with the appropriate columns, see:
:ref:`index-file`, :ref:`index-tutorial`.

::

    write_pkg(pkg, "path/to/pkgdir", idx_df)

The index is always saved as ``index.json``.
