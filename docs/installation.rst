Installation
============

System Requirements
-------------------

* PostgreSQL 9.1 or greater must be installed.
* Python 3.4 or greater. Compatibility with PyPy3.5 is untested and not guaranteed.
* You must use ``psycopg2`` as your database driver.

.. note::

    Due to the way that ``tox`` works with environment setup, if your system's
    Python 3 version is 3.6.x and you installed any Python package that uses
    ``cli-helpers`` version 0.2.0 or greater, ``make setup`` will fail. This is
    due to a `known bug <https://github.com/OCA/pylint-odoo/issues/144>`_ in
    ``pbr`` and as of 2017-12-02 there is no workaround that won't potentially
    break other packages.

Setup
-----

.. code-block:: sh

    pip3 install pytest-pgsql
