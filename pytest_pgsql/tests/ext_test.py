"""Test extension stuff."""

import datetime

import sqlalchemy as sqla
import sqlalchemy.dialects.postgresql as sqla_pg

import pytest_pgsql


jengine = pytest_pgsql.create_engine_fixture('jengine',
                                             json_serializer=lambda _: '{}')
jdb = pytest_pgsql.TransactedPostgreSQLTestDB.create_fixture('jdb', 'jengine')


def test_uses_json(jdb):
    """Verify that the engine we create has a JSON serializer attached."""
    meta = sqla.MetaData(bind=jdb.connection)
    table = sqla.Table('ext_test', meta, sqla.Column('col', sqla_pg.JSON))
    meta.create_all()

    # If we have the serializer set on this engine, we should be able to insert
    # a datetime with no trouble.
    jdb.connection.execute(table.insert(), [    # pylint: disable=no-value-for-parameter
        {'col': datetime.datetime.now()}
    ])

    all_rows = [dict(r) for r in jdb.connection.execute(table.select())]

    assert all_rows == [{'col': {}}]
