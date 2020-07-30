Metadata for Google dataset search
==================================

If a dataset page includes `structured metadata`_ in the ``JSON-LD``
format, it is searchable in Google's `dataset search tool`_.  The
complete list of supported metadata fields is covered in Google's
documentation, so here we will only refer to the properties that might
be relevant for the SENTINEL project.  Typically, a model page will
have two datasets in the SENTINEL archive format: inputs and outputs.
The general metadata between these two should be common, but metadata
specific to the individual files will differ.

.. _`structured metadata`: https://developers.google.com/search/docs/data-types/dataset
.. _`dataset search tool`: https://datasetsearch.research.google.com/


Metadata fields
---------------

- ``name``: descriptive name

- ``description``: short sumamry

- ``alternateName``: machine-friendly name, displaying on the page is
  optional

- ``creator``: list of ``Person``_, and ``Organization``_

- ``citation``: list of related citations, with their own identifiers
  (typically DOIs pointing to the journal article)

- ``identifier``: DOI for the model

- ``keywords``: categories and labels.  This can be hierarchical based
  on a convention of our choosing.  E.g. a model could have keywords
  like these:

  - "Calliope :: euro-calliope"

  - "Language :: Python"

  - "Type :: System Design :: Electricity"

  - "Project :: SENTINEL"

  - "Region :: Europe"

- ``license``: most likely a URL, or a ``CreativeWork``_ object with a
  ``name`` and a ``url``

- ``version``

- ``url``: canonical page describing the model; typically, the current
  page, or the model's own page or repository.

- ``includedInDataCatalog`` (optional): if the model/data is included
  in any catalog like zenodo, that should be mentioned here.

- ``distribution``: downloadable files

  - ``contentUrl``: download url

  - ``encodingFormat``: CSV, Excel, ZIP, etc; for a SENTINEL archive
    most likely a ZIP or Tar.

  - ``name``: Descriptive name of the dataset/archive file

  - ``alternateName``: machine-friendly name, displaying on the page
    is optional

.. _``Person``: https://schema.org/Person
.. _``Organization``: https://schema.org/Organization
.. _``CreativeWork``: https://schema.org/CreativeWork


Page contents
-------------

The entirety of the page content is derived from the above metadata;
in-fact some fields (as indicated above) need not be displayed.
However, to be indexed by Google Dataset Search, the webpage source
should include all of the metadata as a ``JSON-LD`` document.  Here's
an example::

    <script type="application/ld+json">
    {
        "name": "A model of the European power system built using Calliope.",
        "description": "some description",
        "alternateName": "euro-calliope",
        "creator": "Tim Troendle",
        "citation": ["Some article that uses/cites euro-calliope https://doi.org/..."],
        "identifier": "https://doi.org/10.5281/zenodo.3949553",
        "keywords": [
            "Calliope :: euro-calliope",
            "Language :: Python",
            "Type :: System Design :: Electricity",
            "Energy :: Renewable Energy",
            "Project :: SENTINEL",
            "Region :: Europe"
        ],
        "license": {
            "@type": "CreativeWork",
            "name": "MIT",
            "url": "https://github.com/calliope-project/euro-calliope/blob/develop/LICENSE.md"
        },
        "version": 1.0,
        "url": "https://github.com/calliope-project/euro-calliope",
        "distribution": [
            {
                "@type": "DataDownload",
                "name": "Released source code as a ZIP archive",
                "encodingFormat": "application/zip",
                "url": "https://github.com/calliope-project/euro-calliope/archive/v1.0.zip"
            },
            {
                "@type": "DataDownload",
                "name": "Released source code as a gzipped tarball",
                "encodingFormat": "application/gzip",
                "url": "https://github.com/calliope-project/euro-calliope/archive/v1.0.tar.gz"
            },
            {
                "@type": "DataDownload",
                "name": "Pre-built model from Zenodo",
                "encodingFormat": "application/zip",
                "url": "https://zenodo.org/record/3949553/files/pre-built-euro-calliope.zip?download=1"
            }
        ]
    }
    </script>

Here, I have chosen the *euro-calliope* model as illustration.  You
will note, some flexibility is desired for some fields; e.g. each
``citation`` could also be a ``CreativeWork``, and similarly the
``license`` could be a bare URL to the license.

And the distinction between input and output discussed earlier, can be
resolved in ``distribution``, e.g. in the example above the
distinction is between *source code* and *pre-built*.
