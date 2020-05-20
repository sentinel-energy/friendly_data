Persistence
-----------

Once a datapackage has been created, it can be saved to disk as a
*JSON* file, typically named ``datapackage.json``.  Alongside this, a
datapackage consists of other resources like the data files, and other
resources referred in the package description.  These other resources
need to be at the same relative path, otherwise we will not be able to
read the datapackage correctly.  So for sharing datapackages, we
either have the option of sharing them as public repositories,
preserving the directory hierarchy, or bundle everything in archive
files like *ZIP*.  You can use the ``write_pkg`` function to do
this. ::

    from sark.dpkg import write_pkg

    # create package: `pkg`
    write_pkg(pkg, "path/to/datapackage.json")
    # or
    write_pkg(pkg, "path/to/mydatapackage.zip")

The *ZIP* archive preserves the relative directory hierarchy of the
datapackage, as shown in the example below::

    $ unzip -l /tmp/mydatapackage.zip
    Archive:  /tmp/mydatapackage.zip
      Length      Date    Time    Name
    ---------  ---------- -----   ----
        14013  05-07-2020 17:00   data/sample-ok-1.csv
        15446  05-07-2020 17:00   data/sample-ok-2.csv
         1741  05-18-2020 17:23   datapackage.json
    ---------                     -------
        31200                     3 files
