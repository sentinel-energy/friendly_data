.. _registry:

Data registry
=============

Friendly data adds functionality specific to the energy modelling
community via the registry.  It is a collection of metadata (column
schemas) for commonly used data columns.  Each column must have a
generic name, and a data type (a complete list of all supported data
types can found in the `frictionless documentation`_).  Additionally
the registry also records metadata like ``constraints``.  Some
constraints, like ``enum``, where you limit the allowed values in a
column to a valid set, depends on the dataset.  These are mentioned in
the registry, but the value is left blank, and is determined from the
dataset at runtime.

.. toctree::
   :maxdepth: 1

   api/registry
   api/index-file

..
   api/registry is auto-generated during a CI run

Contributing to the registry
-----------------------------

The column registry is designed to evolve as per the needs of the
community.  If you feel it needs to include new columns to express
your dataset/model outputs better, please suggest additions by opening
an `issue`_ on GitHub.  Here we will go over some concepts to make
contributing to the registry easier.

Since columns in tabular datasets can be classified as index columns
and value columns, the registry also respects these two distinctions.
Index columns have values that can identify a row uniquely in a
dataset (like a unique ID), whereas value columns simply contain the
data in question.  A dataset may contain multiple index and value
columns.

Any new column suggestion should be in the YAML format, and should
match the following structure:

.. code-block:: yaml

   name: <column_name>
   type: <type>
   constraints:  # optional
     ...

   description: >-
     Free text description of the column.  This can include
     restructured text syntax for simple formatting.  This text
     will be included in the online documentation.

The ``name`` and ``type`` properties are mandatory, the others are
optional.  However, it is highly recommended that you also include a
concise but complete description.

If you want to specify ``constraints``, you can find a complete list
of all supported properties on the `frictionless documentation`_ page.

Where to add the new column?
++++++++++++++++++++++++++++

As discussed above, there are two kinds of columns, and in the
repository they are separated into two folders.  Index column
definitions should be included in the `idxcols`_ folder, and value
column definitions should be in the `cols`_ folder.

Go to GitHub and open an `issue`_.  Notice that there is an issue
template with a summary of the above information.  Once an issue has
been filed, a pull-request where the YAML file is added to the
appropriate folder in the repository has to be opened.  This can be
done easily by navigating to the desired directory and creating a new
file and typing in the contents.  Please note the issue number in the
pull-request comment field, and link it from the sidebar.  The
community can review the proposal, and suggest edits once an
issue/pull-request has been openned.

Following an agreement, the change can be accepted (or merged), and
the maintainers of the friendly data registry can release an update
with your contribution!

.. _`idxcols`: https://github.com/sentinel-energy/friendly_data_registry/tree/master/friendly_data_registry/idxcols
.. _`cols`: https://github.com/sentinel-energy/friendly_data_registry/tree/master/friendly_data_registry/cols

.. _`frictionless documentation`: https://specs.frictionlessdata.io/table-schema/
.. _`issue`: https://github.com/sentinel-energy/friendly_data_registry/issues

Defining a custom registry
--------------------------

You can define a custom registry in your config file by adding a
``registry`` section.  It can have custom registry definitions under
the ``cols`` and ``idxcols`` keys.  When using the CLI, you can
augment the default registry with your custom registry by passing the
config file as an option.  An example section could look like this:

.. code-block:: yaml

   registry:
     idxcols:
       - name: enduse
         type: string
         constraints:
           enum:
             - cooling
             - heating
             - hot_water
     cols:
       - name: capacity_factor
         type: number
         constraints:
           maximum: 100

When using the Python API, you can temporarily update the registry by
using a context manager.  There are many ways to do this:

- reading a config file
- passing the registry updates as a dictionary,
- passing a list of columns as index columns, or value columns.

For all these options, the modifications are merged with the default
registry, so it is like an update rather than replace.

.. code-block:: python

   from friendly_data.registry import config_ctx, get, getall

   with config_ctx(conffile="config.yaml") as _:
       print(get("enduse", "idxcols"))
       print(getall())
