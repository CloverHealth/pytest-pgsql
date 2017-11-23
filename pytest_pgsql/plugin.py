"""This forms the core of the pytest plugin."""

import sys
import urllib.parse

import pytest
import testing.postgresql

from pytest_pgsql import database
from pytest_pgsql import ext

if sys.version_info < (3, 4):
    # Python <= 3.3 doesn't have importlib.util.find_spec so we need to use the
    # find_module() function. It was deprecated in 3.4 and shouldn't be used in
    # later versions.
    from importlib import find_loader as find_module
else:
    # 3.4+ uses a different function to find packages. Once we drop support for
    # 3.3 then we must use this function exclusively.
    from importlib.util import find_spec as find_module


def pytest_addoption(parser):
    """Add configuration options for pytest_pgsql."""
    parser.addoption(
        '--pg-extensions', action='store', default='',
        help="A comma-separated list of PostgreSQL extensions to install at "
             "the beginning of the session for use by all tests. Example: "
             "--pg-extensions=uuid-ossp,pg_tgrm,pgcrypto")

    parser.addoption(
        '--pg-work-mem', type=int, default=32,
        help='Set the value of the `work_mem` setting, in megabytes. '
             '`pytest_pgsql` defaults to 32. Adjusting this up or down can '
             'help performance; see the Postgres documentation for more details.')

    parser.addoption(
        '--pg-driver', default='psycopg2',
        help="The name of your database driver. If psycopg2 isn't your driver "
             "(e.g. you use pygresql) then you need to pass this argument "
             "providing the name of your driver package so that SQLAlchemy can "
             "use the right one. If not given, pytest_pgsql will fall back "
             "to psycopg2 if possible. pg8000 is explicitly NOT supported.")


@pytest.fixture(scope='session')
def database_uri(request):
    """A fixture giving the connection URI of the session-wide test database."""
    # Note: due to the nature of the variable configs, the command line options
    # must be tested manually.

    work_mem = request.config.getoption('--pg-work-mem')
    if work_mem < 0:    # pragma: no cover
        return pytest.exit('ERROR: --pg-work-mem value must be >= 0. Got: %d'
                           % work_mem)
    elif work_mem == 0:  # pragma: no cover
        # Disable memory tweak and use the server default.
        work_mem_setting = ''
    else:
        # User wants to change the working memory setting.
        work_mem_setting = '-c work_mem=%dMB ' % work_mem

    # pylint: disable=bad-continuation,deprecated-method
    with testing.postgresql.Postgresql(
        postgres_args='-c TimeZone=UTC '
                      '-c fsync=off '
                      '-c synchronous_commit=off '
                      '-c full_page_writes=off '
                      + work_mem_setting +
                      '-c checkpoint_timeout=30min '
                      '-c bgwriter_delay=10000ms') as pgdb:
        driver = request.config.getoption('--pg-driver')
        if driver == 'pg8000':  # pragma: no cover
            raise ValueError(
                "pg8000 is currently unsupported because of how it executes "
                "prepared statements. Please use psycopg2 or a similar driver.")
        elif find_module(driver) is None:  # pragma: no cover
            # Throw an error here to avoid really cryptic error messages from
            # SQLAlchemy when the driver isn't found.
            raise ImportError("Can't find the database driver %r." % driver)

        # Break the connection URL into its parts, replace the protocol part
        # ("postgresql") to define the driver (e.g. "postgresql+pygresql") and
        # return that to the user.
        url_parts = list(urllib.parse.urlparse(pgdb.url()))
        url_parts[0] = 'postgresql+' + driver
        yield urllib.parse.urlunparse(url_parts)


#: A SQLAlchemy engine shared by the transacted and non-transacted database fixtures.
#:
#: .. seealso:: `pytest_pgsql.ext.create_engine_fixture`
# pylint: disable=invalid-name
pg_engine = ext.create_engine_fixture('pg_engine', scope='session')
# pylint: enable=invalid-name


@pytest.fixture(scope='session')
def database_snapshot(pg_engine):
    """Create one database snapshot for the session.

    The database will be restored to this state after each test.

    .. note ::

        This is an implementation detail and should not be used directly except
        by derived fixtures.
    """
    return database.create_database_snapshot(pg_engine)


# pylint: disable=invalid-name

#: Create a test database instance and cleans up after each test finishes.
#:
#: You should prefer the `transacted_postgresql_db` fixture unless your test
#: cannot be run in a single transaction. The `transacted_postgresql_db` fixture
#: leads to faster tests since it doesn't tear down the entire database between
#: each test.
postgresql_db = \
    database.PostgreSQLTestDB.create_fixture('postgresql_db')


#: Create a test database instance that rolls back the current transaction after
#: each test finishes, verifying its integrity before returning.
#:
#: Read the warning in the main documentation page before using this fixture.
transacted_postgresql_db = \
    database.TransactedPostgreSQLTestDB.create_fixture('transacted_postgresql_db')

# pylint: enable=invalid-name
