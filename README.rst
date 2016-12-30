===============================
s3workers
===============================


.. image:: https://img.shields.io/pypi/v/s3workers.svg
        :target: https://pypi.python.org/pypi/s3workers

.. image:: https://img.shields.io/travis/bradrf/s3workers.svg
        :target: https://travis-ci.org/bradrf/s3workers

.. image:: https://readthedocs.org/projects/s3workers/badge/?version=latest
        :target: https://s3workers.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/bradrf/s3workers/shield.svg
     :target: https://pyup.io/repos/github/bradrf/s3workers/
     :alt: Updates


Helper to simplify concurrent access to object scanning in AWS S3 buckets.


* Free software: MIT license
* Documentation: https://s3workers.readthedocs.io.


Features
--------

S3workers provides faster list and delete operations on S3 buckets by opening up simultaneous
connections to issue distinct sets of shared prefix queries. Effectively, this splits up the query
space into 36 independent queries (26 alpha and 10 numeric prefixes). For example, a request to list
all objects in the ``myfancy/`` bucket would result in concurrent list queries to S3 for everything
from ``myfancy/a...`` through ``myfancy/b...`` and everything from ``myfancy/0...`` through
``myfancy/9...``, all at the same time, reporting and collating the results locally.

Selection
~~~~~~~~~

The default output of s3workers is to simply list (or delete) all objects found at the prefix
requested. However, often it is advantageous to restrict the output to only those matching certain
criteria. The ``--select`` option provides the ability for evaluating matches using any normal
Python operators or builtins against one or more of the following variables provided to the selector
for each object found:

* ``name``: The full S3 key name, everything *except* the bucket name (string)
* ``size``: The number of bytes as used by the S3 object (integer).
* ``md5``: The MD5 hash of the S3 object (string).
* ``last_modified``: The timestamp indicating the last time the S3 object was changed (string).

Reduction
~~~~~~~~~

In cases where aggregation of some kind is desired, s3workers provides the ability to execute
reduction logic against an accumulator value. For example, to produce a sum of the size of all
selected S3 objects or to even group the size according to MD5 values. See the usage output for
examples. In all cases, the same variables provided by selection are also provided when reducing.


Credits
---------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project
template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
