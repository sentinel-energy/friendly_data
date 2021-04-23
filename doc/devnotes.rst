Notes for developers
====================

Adding dependencies
-------------------

- update list of mocked modules in ``doc/conf.py``
- may need to update patch to ``requirements*.txt``
  (``dev/requirements*.patch``) files for CI

Working with the data package API
---------------------------------

- workaround for POSIX compliant paths on Windows
- metadata extension:
  - always use ``alias``-aware functions like
    ``friendly_data.dpkg.get_aliased_cols``
- fill index levels (``enum`` values) using
  ``friendly_data.dpkg.index_levels``
