The configuration file
----------------------

The config file used by the Friendly data CLI is a standard YAML file,
with the following top-level keys.

**metadata** (mapping or dictionary)

    This section should include the metadata of the data package.
    Some attributes are mandatory (mentioned below), some optional
    attributes are also included below, however for a comprehensive
    list see the upstream documentation for `frictionless`_.

    **name** (string, mandatory)

        A short, unique name, with no spaces or special characters that is URL
        friendly.  Once a data package is published, this name should
        not change even when you are releasing a new version.

    **licenses** (list of strings, mandatory)

        A list of licenses that apply to your data package.  Valid
        license names can be found using the CLI by running the
        command ``friendly_data list-licenses``.  The ``id`` field is
        what you should use.  This makes sure other relevant metadata
        related to the license is pulled in (e.g a URL) and included
        in the data package.  In case only one license applies to you,
        you can provide a single string, which will be converted to a
        list with a single entry when reading the config file.

	.. code-block:: yaml

	   metadata:
             licenses: [CC0-1.0]

    **keywords** (list of strings)

        A list of keywords to describe the data package

	.. code-block:: yaml

	   metadata:
	     keywords:
	       - energy system design
               - europe
               - sentinel

    **description** (string)

        A human readable, free text, long description.

**indices** (mapping or dictionary)

    This section defines different indices required for converting a
    data package to IAMC format.  There are two kinds of index columns
    in the context of converting to IAMC format: columns that are
    mandatory, and columns that are user defined whose values are
    substituted in the IAMC variable format string.

    This section should be mapping, where each key is one of these
    index columns.  However, for the mandatory IAMC columns, the value
    represents a default in case that particular column is missing in
    a dataset, whereas for user defined index columns, it should point
    to a CSV file that defines the different levels (or values), and
    their corresponding IAMC name.

    .. code-block:: yaml

       indices: 
         technology: technology.csv
         model: calliope
         scenario: diag-npi
         year: 2030

    In the above example, the mandatory IAMC columns *model*,
    *scenario*, and *year* are set to the specified defaults.  On the
    other hand, the *technology* column is associated to a CSV files
    (shown below).

    .. csv-table:: Technology definitions and their IAMC names
       :file: _static/data/technology.csv
       :header-rows: 1

    This file should have two columns, the first column called *name*
    should have list the different levels that are present in the
    datasets.  It's not necessary that all datasets have all levels,
    this should be a union of all levels for all datasets in the data
    package.  The second column should be called *iamc* and it defines
    the IAMC names that is used when the values are substituted in the
    IAMC variable format string.

**registry** (mapping or dictionary)

    Custom additions/update to the default registry.  It could look
    something like this:

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

.. _`frictionless`: https://specs.frictionlessdata.io/data-package/#metadata
