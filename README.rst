Clean PostgreSQL Databases for Your Tests
=========================================

The following is a summary of the complete pytest_pgsql docs, which are available
on `ReadTheDocs <http://pytest-pgsql.readthedocs.io/>`_.

What is ``pytest_pgsql``?
------------------------------

``pytest_pgsql`` is a `pytest <https://pytest.org>`_ plugin you can use to
write unit tests that utilize a temporary PostgreSQL database that gets cleaned
up automatically after every test runs, allowing each test to run on a completely
clean database (with some limitations).

The plugin gives you two fixtures you can use in your tests: ``postgresql_db`` and
``transacted_postgresql_db``. Both of these give you similar interfaces to access
to the database, but have slightly different use cases (see below).

Sample Usage
------------

You can use a session, connection, or engine - the choice is up to you.
``postgresql_db`` and ``transacted_postgresql_db`` both give you a session, but
``postgresql_db`` exposes its engine and ``transacted_postgresql_db`` exposes its
connection::

    def test_orm(postgresql_db):
        instance = Person(name='Foo Bar')
        postgresql_db.session.add(instance)
        postgresql_db.session.commit()
        with postgresql_db.engine.connect() as conn:
            do_thing(conn)

    def test_connection(transacted_postgresql_db):
        instance = Person(name='Foo Bar')
        transacted_postgresql_db.session.add(instance)
        transacted_postgresql_db.session.commit()

        transacted_postgresql_db.connection.execute('DROP TABLE my_table')

Features
--------

The following is a non-exhaustive list of some of the features provided to you
by the database fixtures.

Manipulating Time
~~~~~~~~~~~~~~~~~

Both database fixtures use `freezegun <https://github.com/spulec/freezegun>`_ to
allow you to freeze time inside a block of code. You can use it in a variety of
ways:

As a context manager::

    with postgresql.time.freeze('December 31st 1999 11:59:59 PM') as freezer:
        # Time is frozen inside the database *and* Python.
        now = postgresql_db.session.execute('SELECT NOW()').scalar()
        assert now.date() == datetime.date(1999, 12, 31)
        assert datetime.date.today() == datetime.date(1999, 12, 31)

        # Advance time by 1 second so we roll over into the new year
        freezer.tick()

        now = postgresql_db.session.execute('SELECT NOW()').scalar()
        assert now.date() == datetime.date(2000, 1, 1)

As a decorator::

    @pytest_pgsql.freeze_time(datetime.datetime(2038, 1, 19, 3, 14, 7))
    def test_freezing(postgresql_db):
        today = postgresql_db.session.execute(
            "SELECT EXTRACT('YEAR' FROM CURRENT_DATE)").scalar()
        assert today.year == 2038
        assert datetime.date.today() == datetime.date(2038, 1, 19)

And more!

General-Purpose Functions
~~~~~~~~~~~~~~~~~~~~~~~~~

``postgresql_db`` and ``transacted_postgresql_db`` provide some general-purpose
functions to ease test setup and execution.

- ``load_csv()`` loads a CSV file into an existing table.
- ``run_sql_file()`` executes a SQL script, optionally performing variable binding.

Extension Management
~~~~~~~~~~~~~~~~~~~~

Since version 9.1 Postgres supports `extensions <https://www.postgresql.org/docs/current/static/external-extensions.html>`_.
You can check for the presence of and install extensions like so::

    >>> postgresql_db.is_extension_available('asdf')  # Can I install this extension?
    False
    >>> postgresql_db.is_extension_available('uuid-ossp')  # Maybe this one is supported...
    True
    >>> postgresql_db.install_extension('uuid-ossp')
    True
    >>> postgresql_db.is_extension_installed('uuid-ossp')
    True

``install_extension()`` has additional arguments to allow control over which schema
the extension is installed in, what to do if the extension is already installed,
and so on. See the documentation for descriptions of these features.

Schemas and Tables
~~~~~~~~~~~~~~~~~~

You can create `table schemas <https://www.postgresql.org/docs/current/static/ddl-schemas.html>`_
by calling ``create_schema()`` like so::

    postgresql_db.create_schema('foo')          # Create one schema
    postgresql_db.create_schema('foo', 'bar')   # Create multiple ones

To quickly see if a table schema exists, call ``has_schema()``::

    >>> postgresql_db.has_schema('public')
    True

Similarly, you can create tables in the database with ``create_table()``. You can
pass SQLAlchemy ``Table`` instances or ORM declarative model classes::

    # Just a regular Table.
    my_table = Table('abc', MetaData(), Column('def', Integer()))

    # A declarative model works too.
    class MyORMModel(declarative_base()):
        id = Column(Integer, primary_key=True)

    # Pass a variable amount of tables to create
    postgresql_db.create_table(my_table, MyORMModel)

Installation
============

Sorry, this library is not compatible with Python 2. Please be sure to use ``pip3`` instead of
``pip`` when installing::

    pip3 install pytest-pgsql


Contributing Guide
==================

For information on setting up pytest_pgsql for development and contributing
changes, view `CONTRIBUTING.rst <CONTRIBUTING.rst>`_.
