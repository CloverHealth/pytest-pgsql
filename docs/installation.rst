Installation
============

System Requirements
-------------------

* PostgreSQL 9.1 or greater must be installed.
* Python 3.3 or greater. Compatibility with PyPy3.5 is untested and not guaranteed.
* You must use ``psycopg2`` as your database driver.

.. note::

    Due to the way that ``tox`` works with environment setup, if your system's
    Python 3 version is 3.6.x and you install `pgcli <https://www.pgcli.com/>`_,
    ``make setup`` will fail. This is due to a `known bug <https://github.com/OCA/pylint-odoo/issues/144>`_
    in ``pbr`` and as of 2017-12-01 there is no workaround that doesn't break
    ``pgcli``. You'll need to uninstall it.

Setup
-----

.. code-block:: sh

    pip3 install pytest-pgsql
