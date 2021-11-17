How to install?
---------------

You can install (or update) the package with ``pip``::

  $ pip install [-U] friendly-data
    
As the registry is coupled loosely, so that it can be updated much
faster.  This will accomodate to an evolving data vocabulary of the
community.  So it is recommended to update the registry regularly, eventhough you might not want to update the main package.  You can do this with::

  $ pip install -U friendly-data-registry

If your workflow also includes converting your data packages into IAMC
format, you will need the optional dependencies; which can be
installed at any time like this (leaving out the `-U` will not upgrade
the main package)::

  $ pip install [-U] friendly-data[extras]
