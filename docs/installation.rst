Installation
============

System Requirements
-------------------

* PostgreSQL 9.1 or greater must be installed.
* Python 3.4 or greater. Compatibility with PyPy3.5 is untested and not guaranteed.
* A database driver supported by SQLAlchemy must also be installed (see full list
  `here <http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html#dialect-postgresql>`_).
  `psycopg2 <http://initd.org/psycopg/>`_ is guaranteed to work;
  `pg8000 <https://github.com/mfenniak/pg8000/>`_ on the other hand is known to
  be incompatible with pytest-pgsql.

Setup
-----

pip3 install pytest-pgsql
