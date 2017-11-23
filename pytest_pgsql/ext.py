"""Facilities for extending the database."""

import pytest
import sqlalchemy as sqla


def create_engine_fixture(name, scope='session', **engine_params):
    """A factory function that creates a fixture with a customized SQLAlchemy
    :class:`~sqlalchemy.engine.Engine`.

    Because setup and teardown will require additional time and resources if
    you're using both a custom *and* the default engine, if you need this engine
    in more than one module you might want to consider using this scoped at the
    session level, i.e. initialized and torn down once for the entire test run.
    The tradeoff is that if you use multiple engines, each custom one will use
    additional resources such as connection pools and memory for the entirety of
    the session. If you only need this custom engine in a few places, it may be
    more resource-efficient to scope this to an individual test, class, or
    module.

    Any extensions declared using the ``--pg-extensions`` command-line option
    will be installed as part of this engine's setup process.

    .. warning::
        Because an engine performs no cleanup itself, any changes made with an
        engine fixture directly are *not* rolled back and can result in the
        failure of other tests (usually with a
        :class:`~pytest_pgsql.errors.DatabaseIsDirtyError` at teardown).
        You should only use this in conjunction with
        :meth:`~pytest_pgsql.database.PostgreSQLTestDBBase.create_fixture` to
        create a *database* fixture that you'll use. Engine fixtures shouldn't
        be used directly.

    Arguments:
        name (str):
            The name of the fixture. It must be unique, so ``pg_engine`` is not
            allowed.

        scope (str):
            The scope that this customized engine should have. Valid values are:

            * ``class``: The engine is initialized and torn down for each test
              class that uses it.
            * ``function``: The engine is initialized and torn down for each
              test that uses it.
            * ``module``: The engine is initialized and torn down once per
              module that uses it.
            * ``session``: The engine is initialized and torn down once per
              pytest run.

            Default: ``session``.

        **engine_params:
            Keyword arguments to pass to :func:`sqlalchemy.create_engine`. (You
            cannot change the connection URL with this.)

    Usage:

        .. code-block:: python

            # conftest.py
            import simplejson as json

            # Create an engine fixture named `jengine`
            jengine = pytest_pgsql.create_engine_fixture(
                'jengine', json_serializer=json.dumps, json_deserializer=json.loads)

            # Create a new database fixture that uses our `jengine`.
            jdb = pytest_pgsql.PostgreSQLTestDB.create_fixture('jdb', 'jengine')

            # ----------------
            # test_json.py
            import datetime
            import sqlalchemy as sqla
            import sqlalchemy.dialects.postgresql as sqla_pg

            def test_blah(jdb):
                meta = sqla.MetaData(bind=jdb.connection)
                table = sqla.Table('test', meta, sqla.Column('col', sqla_pg.JSON))
                meta.create_all()

                jdb.connection.execute(table.insert(), [
                    {'col': datetime.datetime.now()}
                ])
    """
    @pytest.fixture(name=name, scope=scope)
    def _engine_fixture(database_uri, request):
        engine = sqla.create_engine(database_uri, **engine_params)
        quote_id = engine.dialect.preparer(engine.dialect).quote_identifier

        opt_string = request.config.getoption('--pg-extensions')
        to_install = (s.strip() for s in opt_string.split(','))

        query_string = ';'.join(
            'CREATE EXTENSION IF NOT EXISTS %s' % quote_id(ext)
            for ext in to_install if ext)

        if query_string:    # pragma: no cover
            engine.execute('BEGIN TRANSACTION; ' + query_string + '; COMMIT;')

        yield engine
        engine.dispose()

    return _engine_fixture
