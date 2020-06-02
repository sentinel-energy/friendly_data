Our objectives are to develop a standard model and data specification
through which the models in SENTINEL can be linked together, including
inputs and outputs, and to classify the models according to this
specification.

The primary purpose of the interface framework is to allow data to
flow between very different types of models, in a way that can be
automated. This is challenging because not only do different models
use different data formats, but also, metadata may or may not exist,
and conventions on units or variable naming differ between different
models and research communities.

.. figure:: ecosystem.png
   :width: 90%
   :align: center

   An open ecosystem enabled by the data format, where different
   modelling frameworks can be interlinked, and a suit of independent
   software tools to work with data can be developed.

To address this, we develop a flexible data standard that can form the
glue between different models, allowing them to be used together, and
a user interface that makes it easier for modelling teams to actually
make use of this data format.  All software described here is
available as open source software under the version 2 of the `Apache
software license`_.

.. _`Apache software license`: https://www.apache.org/licenses/LICENSE-2.0
