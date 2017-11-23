"""Common fixtures and functions for use in tests."""

import contextlib

import pytest

from pytest_pgsql import plugin


@contextlib.contextmanager
def check_teardown(fixture, execute):
    yield fixture

    # Teardown hasn't been executed yet so we need to trigger it ourselves.
    fixture.rollback()
    fixture.reset_db()

    assert not fixture.is_dirty()


@pytest.fixture
def clean_tpgdb(transacted_postgresql_db):  # pragma: no cover
    """A transacted_postgresql_db fixture that verifies its cleanliness...ish."""
    execute = transacted_postgresql_db.connection.execute
    with check_teardown(transacted_postgresql_db, execute) as fixture:
        yield fixture


@pytest.fixture
def clean_pgdb(postgresql_db):  # pragma: no cover
    """A postgresql_db fixture that verifies its cleanliness...ish."""
    execute = postgresql_db.engine.execute
    with check_teardown(postgresql_db, execute) as fixture:
        yield fixture


@pytest.fixture(params=['non-transacted', 'transacted'])
def clean_db(database_uri, request):
    """Generic database - run a test with both the transacted and non-transacted
    databases."""
    if request.param == 'transacted':
        yield from plugin.transacted_postgresql_db(database_uri, request)
    else:
        yield from plugin.postgresql_db(database_uri, request)
