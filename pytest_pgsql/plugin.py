"""This forms the core of the pytest plugin."""

import pytest
import testing.postgresql

from pytest_pgsql import database
from pytest_pgsql import ext


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


@pytest.fixture(scope='session')
def database_uri(request):
    """A fixture giving the connection URI of the session-wide test database."""
    # Note: due to the nature of the variable configs, the command line options
    # must be tested manually.

    work_mem = request.config.getoption('--pg-work-mem')
    if work_mem < 0:    # pragma: no cover
        pytest.exit('ERROR: --pg-work-mem value must be >= 0. Got: %d' % work_mem)
        return
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
        yield pgdb.url()


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
