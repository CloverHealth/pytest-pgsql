"""Tests for the database."""

import csv
import datetime
import io
import random
import tempfile

import pytest_pgsql.time
from pytest_pgsql import errors

import pytest
import sqlalchemy as sqla
import sqlalchemy.engine as sqla_eng
import sqlalchemy.exc as sqla_exc
import sqlalchemy.ext.declarative as sqla_decl
from sqlalchemy import func as sqla_func
import sqlalchemy.sql as sqla_sql

DeclBase = sqla_decl.declarative_base()


class BasicModel(DeclBase):
    """A trivial ORM model for testing stuff."""
    __tablename__ = 'basic_model'
    id = sqla.Column(sqla.Integer, primary_key=True)
    value = sqla.Column(sqla.Integer)


BASIC_TABLE = sqla.Table(
    'basic_table',
    sqla.MetaData(),
    sqla.Column('id', sqla.Integer),
    sqla.Column('value', sqla.Integer))


# We need to put this table in a separate schema to ensure that table names with
# schemas are properly handled in the truncate statement created by load_csv().
REFERENCED_TABLE = sqla.Table(
    'referenced_table',
    sqla.MetaData(),
    sqla.Column('id', sqla.Integer, primary_key=True, autoincrement=True),
    sqla.Column('value', sqla.Integer),
    schema='some_schema')


REFERRING_TABLE = sqla.Table(
    'referring_table',
    sqla.MetaData(),
    sqla.Column('id', sqla.Integer, primary_key=True, autoincrement=True),
    sqla.Column('ref_id', sqla.ForeignKey(REFERENCED_TABLE.c.id)))


def random_identifier(prefix='_'):
    return prefix + '%06d' % random.randrange(1E7)


def get_basictable_rowcount(session, table=BASIC_TABLE):
    """Return the number of rows in BASIC_TABLE."""
    count_query = sqla_sql.select([sqla_func.count()]).select_from(table)
    return session.execute(count_query).scalar()


@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
def test_create_schema(clean_db, conn_name):
    """Create a schema in the test database."""
    schema_name = random_identifier()
    clean_db.create_schema(schema_name)

    conn = getattr(clean_db, conn_name)
    query = sqla.text(
        'SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=:name LIMIT 1)'
    ).bindparams(name=schema_name)

    assert conn.execute(query).scalar() == 1


def test_create_multiple_schemas(clean_db):
    """Create multiple schemas in the test database."""
    schema_names = ['schema_%s' % i for i in range(5)]
    clean_db.create_schema(*schema_names)

    query = sqla.text(
        'SELECT COUNT(*) FROM pg_namespace WHERE nspname IN :names'
    ).bindparams(names=tuple(schema_names))

    assert clean_db.session.execute(query).scalar() == len(schema_names)


def test_create_schema_no_injection(clean_db):
    """Verify that create_schema is invulnerable to SQL injection."""
    # Create a table we can maliciously drop
    table = sqla.Table('test', sqla.MetaData(), sqla.Column('id', sqla.Integer))
    clean_db.create_table(table)
    assert clean_db.has_table(table)

    # Try creating a schema that will result in the following query:
    #   CREATE SCHEMA "foo"; DROP TABLE test CASCADE; --"
    malicious_schema = 'foo"; DROP TABLE test CASCADE; --'
    clean_db.create_schema(malicious_schema)

    # Our table should be intact, and there should be no schema called "foo".
    assert clean_db.has_table(table)
    assert clean_db.has_schema(malicious_schema)
    assert not clean_db.has_schema('foo')


@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
def test_has_schema(clean_db, conn_name):
    schema = random_identifier()
    clean_db.create_schema(schema)
    conn = getattr(clean_db, conn_name)

    # Make sure we get the same result executing the query directly and using
    # our function.
    query = sqla.text(
        'SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=:name LIMIT 1)'
    ).bindparams(name=schema)

    assert conn.execute(query).scalar() is True
    assert clean_db.has_schema(schema)

    exists = conn.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='bogus' LIMIT 1)"
    ).scalar()

    assert exists is False
    assert not clean_db.has_schema('bogus')


@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
def test_has_table(clean_db, conn_name):
    schema_name = random_identifier()
    table_name = random_identifier()
    conn = getattr(clean_db, conn_name)

    table = sqla.Table(
        table_name,
        sqla.MetaData(bind=conn),
        sqla.Column('id', sqla.Integer, primary_key=True),
        schema=schema_name)

    clean_db.create_schema(schema_name)
    clean_db.create_table(table)

    # Test all three ways to check for this table - the Table object, the table
    # name with the schema, and the table name without the schema.
    assert clean_db.has_table(table)
    assert clean_db.has_table(table.fullname)
    assert clean_db.has_table(table_name)

    assert not clean_db.has_table('bogus_table')

    with pytest.raises(TypeError) as errinfo:
        clean_db.has_table(0)

    assert str(errinfo.value) == \
        "Expected str, SQLAlchemy Table, or declarative model, got 'int'."


def test_reset_db_transacted(clean_db):
    """Verify reset_db() blows away schemas we created in the transacted DB."""
    schema = random_identifier()
    clean_db.create_schema(schema)
    clean_db.install_extension('pgcrypto')
    assert clean_db.has_schema(schema)


def test_create_table(clean_db):
    """Create a single table."""
    table = sqla.Table(
        'test_table',
        sqla.MetaData(),
        sqla.Column('id', sqla.Integer, primary_key=True))

    assert not clean_db.has_table('test_table')
    clean_db.create_table(table)
    assert clean_db.has_table('test_table')


def test_create_multiple_tables(clean_db):
    """Create multiple tables."""
    tables = [
        sqla.Table(
            'test_table_%s' % i,
            sqla.MetaData(),
            sqla.Column('id', sqla.Integer, primary_key=True))
        for i in range(5)
    ]

    for table in tables:
        assert not clean_db.has_table(table.fullname)

    clean_db.create_table(*tables)

    for table in tables:
        assert clean_db.has_table(table.fullname)


def test_create_decl_table(clean_db):
    """Create a declarative ORM model in the database."""
    assert not clean_db.has_table(BasicModel)
    assert not clean_db.has_table(BasicModel.__table__)
    assert not clean_db.has_table(BasicModel.__tablename__)

    clean_db.create_table(BasicModel.__table__)

    assert clean_db.has_table(BasicModel)
    assert clean_db.has_table(BasicModel.__table__)
    assert clean_db.has_table(BasicModel.__tablename__)


# NOTE: Don't swap the order of the parameterizations, and don't combine them.
# We need the schema to change multiple times per connection, not change the
# connection multiple times per schema.
@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
@pytest.mark.parametrize('schema', [
    # Each schema name must occur twice consecutively so that SQLAlchemy will
    # explode if we have a collision.
    'my_schema',
    'my_schema',
    'public',
    'public',
])
def test_manual_create_table_teardown(clean_db, conn_name, schema):
    """Tables created manually should be deleted automatically.

    The idea here is to create a table using the connection. The test will end
    and the table should be deleted. After that, the session will attempt to
    create a table with the same name. No exception should be raised.

    We test this with both a custom schema and a preexisting schema to ensure
    that table deletions work for schemas we can't delete.
    """
    # clean_db.create_schema('my_schema')
    # conn = getattr(clean_db, conn_name)
    # conn.execute('CREATE TABLE %s.thing (id SERIAL)' % schema)
    # assert clean_db.has_table('%s.thing' % schema)


@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
def test_has_extension_true_negative(clean_db, conn_name):
    """Verify we can accurately detect an uninstalled extension."""
    assert not clean_db.has_extension('uuid-ossp')

    # If the extension isn't installed, attempting to generate a UUID will fail.
    conn = getattr(clean_db, conn_name)
    with pytest.raises(sqla_exc.DatabaseError):
        conn.execute('SELECT uuid_generate_v4()')


def test_create_extension_no_injection(clean_db):
    """Verify that install_extension is invulnerable to SQL injection."""
    # Try installing an extension that will result in the following query:
    #   CREATE EXTENSION "uuid-ossp"; DROP TABLE IF EXISTS test CASCADE; --"
    malicious_extension = 'uuid-ossp"; DROP TABLE IF EXISTS test CASCADE; --'

    # If we quoted this right then an exception should've been raised because
    # 'uuid-ossp"; DROP TABLE test CASCADE; --' is not a valid extension name.
    # If no exception is raised then the DROP succeeded.
    with pytest.raises(sqla_exc.DataError):
        clean_db.install_extension(malicious_extension)


@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
def test_create_extension(clean_db, conn_name):
    """Verify creating an extension works."""
    assert not clean_db.has_extension('uuid-ossp')

    clean_db.install_extension('uuid-ossp')
    assert clean_db.has_extension('uuid-ossp')

    # This shouldn't blow up if we have the extension installed.
    conn = getattr(clean_db, conn_name)
    conn.execute('SELECT uuid_generate_v4()')


def test_create_extension_in_schema(clean_db):
    """Ensure we can create an extension in a non-default schema."""
    assert not clean_db.has_extension('uuid-ossp')
    clean_db.create_schema('foo')
    clean_db.install_extension('uuid-ossp', schema='foo')
    assert clean_db.has_extension('uuid-ossp')

    # Because has_extension() will return ``True`` wherever the extension is,
    # we have to see if it exists in a different schema another way.
    clean_db.session.execute('SELECT foo.uuid_generate_v4()')


def test_create_extension_exists_ok(clean_db):
    """The exists_ok argument should prevent crashing if an extension exists."""
    assert not clean_db.has_extension('uuid-ossp')
    assert clean_db.install_extension('uuid-ossp') is True
    assert clean_db.has_extension('uuid-ossp')

    # Try to install the extension again. It should blow up if we don't have
    # exists_ok set.
    with pytest.raises(sqla_exc.ProgrammingError):
        clean_db.install_extension('uuid-ossp')

    # Recover from the exception
    clean_db.rollback()

    # Try installing the extension again.
    assert clean_db.install_extension('uuid-ossp', exists_ok=True) is True


def test_create_extension_if_available(clean_db):
    """Using if_available will not install unsupported extensions."""
    assert not clean_db.is_extension_available('asdf')

    # Try to install this bogus exception and make sure we don't swallow the
    # error somehow.
    with pytest.raises(sqla_exc.OperationalError):
        clean_db.install_extension('asdf')

    # Recover from the exception and try to install the extension again.
    clean_db.rollback()
    assert clean_db.install_extension('asfd', if_available=True) is False


@pytest.mark.parametrize('ext_name,result', [
    ('uuid-ossp', True),    # Test with test name requiring quotes
    ('pgcrypto', True),     # Test with test name not requiring quotes
    ('asdfjkl', False),     # This shouldn't exist.
])
def test_has_extension(clean_db, ext_name, result):
    """Verify extension checking works."""
    assert clean_db.is_extension_available(ext_name) is result


def test_reflect_public_table_no_schema(clean_db):
    """Reflecting a table without the schema will succeed IF the table is in the
    search path, e.g. ``public`` or ``pg_catalog``."""
    assert clean_db.has_table('pg_index')
    reflected = clean_db.get_table('pg_index')

    # Verify we can use this reflected table.
    query = sqla_sql.select([sqla_func.count()]).select_from(reflected)
    n_rows = clean_db.session.execute(query).scalar()
    assert n_rows > 0


def test_reflect_table_with_schema(clean_db):
    """Reflect a table with the schema."""
    assert clean_db.has_table('pg_catalog.pg_index')
    reflected = clean_db.get_table('pg_catalog.pg_index')

    # Verify we can use this reflected table.
    query = sqla_sql.select([sqla_func.count()]).select_from(reflected)
    n_rows = clean_db.session.execute(query).scalar()
    assert n_rows > 0


def test_reflect_table_with_metadata(clean_db):
    """Verify the metadata we pass in is bound to the table."""
    meta = sqla.MetaData()
    reflected = clean_db.get_table('pg_catalog.pg_index', meta)
    assert reflected.metadata is meta


@pytest_pgsql.freeze_time('2017-01-01')
def test_run_sql_basic_filename(clean_tpgdb):
    """Test executing a basic SQL file, passing a filename to the function."""
    with tempfile.NamedTemporaryFile('w+') as fd:
        fd.write('SELECT CURRENT_DATE')
        fd.flush()

        result = clean_tpgdb.run_sql_file(fd.name)
        assert isinstance(result, sqla_eng.ResultProxy)
        assert result.scalar() == datetime.date(2017, 1, 1)


@pytest_pgsql.freeze_time('2017-01-01')
def test_run_sql_basic_buffer(clean_tpgdb):
    """Test executing a basic SQL file, passing a buffer to the function."""
    sql_file = io.StringIO('SELECT CURRENT_DATE')

    result = clean_tpgdb.run_sql_file(sql_file)
    assert isinstance(result, sqla_eng.ResultProxy)
    assert result.scalar() == datetime.date(2017, 1, 1)


def test_run_sql_basic_bindings(clean_tpgdb):
    """Test executing a basic SQL file with bindings."""
    sql_file = io.StringIO('SELECT CURRENT_DATE = :date')

    result = clean_tpgdb.run_sql_file(sql_file, date=datetime.date(1970, 1, 1))
    assert isinstance(result, sqla_eng.ResultProxy)
    assert result.scalar() is False


def test_run_sql_transacted_teardown_ok(clean_tpgdb):
    """Verify that teardown still works with the SQL execution in the transacted
    database."""
    sql_file = io.StringIO("""
        CREATE SCHEMA garbage;
        CREATE TABLE garbage.more_garbage(id SERIAL PRIMARY KEY);
        CREATE TABLE public.even_more_garbage(id SERIAL PRIMARY KEY);
    """)

    clean_tpgdb.run_sql_file(sql_file)
    # Assertions done by clean_tpgdb for us


def test_run_sql_teardown_ok(clean_pgdb):
    """Verify that teardown still works with the SQL execution in the regular
    database."""
    sql_file = io.StringIO("""
        CREATE SCHEMA garbage;
        CREATE TABLE garbage.more_garbage(id SERIAL PRIMARY KEY);
        CREATE TABLE public.even_more_garbage(id SERIAL PRIMARY KEY);
    """)

    clean_pgdb.run_sql_file(sql_file)
    # Assertions done by clean_pgdb for us


@pytest.fixture
def basic_csv():
    """Create a CSV file and return the data along with the file descriptor."""
    csv_rows = [{'id': i, 'value': random.randrange(100)} for i in range(10)]

    # We have to use `NamedTemporaryFile` because the fs fixture doesn't appear
    # to work with Pandas (TypeError thrown when opening a file by name).
    with tempfile.NamedTemporaryFile('w+') as fd:
        writer = csv.DictWriter(fd, ('id', 'value'))
        writer.writeheader()
        writer.writerows(csv_rows)
        fd.flush()
        fd.seek(0)
        yield csv_rows, fd


@pytest.mark.parametrize('count_mult,truncate,cascade', (
    (1, True, False),   # Truncate but don't cascade (should be okay for this).
    (1, True, True),    # Truncate and cascade (shouldn't matter).
    (2, False, False),  # Don't truncate, don't cascade.
    (2, False, True),   # Don't truncate, `cascade` should be ignored.
))
def test_load_csv_basic(clean_tpgdb, basic_csv, count_mult, truncate, cascade):
    """Test basic load of a CSV, and that data is appended by default."""
    csv_rows, csv_fd = basic_csv

    clean_tpgdb.create_table(BASIC_TABLE)
    assert clean_tpgdb.has_table(BASIC_TABLE)

    n_inserted = clean_tpgdb.load_csv(csv_fd, BASIC_TABLE)
    assert n_inserted == len(csv_rows)
    assert get_basictable_rowcount(clean_tpgdb.session) == n_inserted

    # Load data from the CSV again. We should now have exactly twice the number
    # of rows, since this is supposed to append by default. Also use the table's
    # name instead of the object itself.
    n_inserted = clean_tpgdb.load_csv(csv_fd.name, BASIC_TABLE.fullname,
                                      truncate=truncate, cascade=cascade)
    assert n_inserted == len(csv_rows)
    assert get_basictable_rowcount(clean_tpgdb.session) == count_mult * len(csv_rows)


def test_load_csv_declarative(clean_tpgdb, basic_csv):
    """Try loading a CSV into a declarative model.

    TODO (dargueta): Somehow integrate this into ``test_load_csv_basic``.
    """
    csv_rows, csv_fd = basic_csv

    clean_tpgdb.create_table(BasicModel)
    assert clean_tpgdb.has_table(BasicModel)

    n_inserted = clean_tpgdb.load_csv(csv_fd.name, BasicModel)
    assert n_inserted == len(csv_rows)
    assert get_basictable_rowcount(clean_tpgdb.session, BasicModel) == n_inserted


def test_load_csv_truncates_table(clean_tpgdb, basic_csv):
    """Verify truncating works when no tables reference the one being loaded."""
    csv_rows, csv_fd = basic_csv

    clean_tpgdb.create_table(BASIC_TABLE)
    assert clean_tpgdb.has_table(BASIC_TABLE)

    # pylint: disable=no-value-for-parameter
    clean_tpgdb.session.execute(BASIC_TABLE.insert().values(csv_rows))
    # pylint: enable=no-value-for-parameter

    # Load data from the CSV again. Because we're truncating we should still
    # have exactly the same number of rows.
    n_inserted = clean_tpgdb.load_csv(csv_fd.name, BASIC_TABLE, truncate=True)
    assert n_inserted == len(csv_rows)
    assert get_basictable_rowcount(clean_tpgdb.session) == len(csv_rows)


@pytest.mark.parametrize('truncate,expected_exc', (
    (False, sqla_exc.IntegrityError),       # Don't truncate -> pkey violation
    (True, sqla_exc.NotSupportedError),     # Truncate but don't cascade -> boom
))
def test_load_csv_to_referenced_table_crash(
        clean_tpgdb, basic_csv, truncate, expected_exc):
    """Verify expected crashes when loading duplicates but not truncating, or
    not cascading when truncating."""
    csv_rows, csv_fd = basic_csv

    clean_tpgdb.create_schema(REFERENCED_TABLE.schema)
    clean_tpgdb.create_table(REFERENCED_TABLE)
    clean_tpgdb.create_table(REFERRING_TABLE)

    assert clean_tpgdb.has_table(REFERENCED_TABLE)
    assert clean_tpgdb.has_table(REFERRING_TABLE)

    # pylint: disable=no-value-for-parameter
    clean_tpgdb.session.execute(REFERENCED_TABLE.insert().values(csv_rows))
    REFERRING_TABLE.insert().values({'id': 1, 'ref_id': 1})
    # pylint: enable=no-value-for-parameter

    # Try loading from the CSV.
    with pytest.raises(expected_exc):
        clean_tpgdb.load_csv(csv_fd.name, REFERENCED_TABLE, truncate=truncate)


def test_load_csv_to_referenced_table_ok(clean_tpgdb, basic_csv):
    """Ensure truncation cascades to referring tables."""
    csv_rows, csv_fd = basic_csv

    clean_tpgdb.create_table(REFERENCED_TABLE)
    clean_tpgdb.create_table(REFERRING_TABLE)

    assert clean_tpgdb.has_table(REFERENCED_TABLE)
    assert clean_tpgdb.has_table(REFERRING_TABLE)

    # pylint: disable=no-value-for-parameter
    clean_tpgdb.session.execute(REFERENCED_TABLE.insert().values(csv_rows))
    REFERRING_TABLE.insert().values({'id': 1, 'ref_id': 1})
    # pylint: enable=no-value-for-parameter

    # Load data from the CSV again. Because we're truncating we should still
    # have exactly the same number of rows.
    n_inserted = clean_tpgdb.load_csv(csv_fd.name, REFERENCED_TABLE,
                                      truncate=True, cascade=True)
    assert n_inserted == len(csv_rows)
    assert get_basictable_rowcount(clean_tpgdb.session, REFERENCED_TABLE) == len(csv_rows)
    assert get_basictable_rowcount(clean_tpgdb.session, REFERRING_TABLE) == 0


@pytest.mark.parametrize('create_stmt,drop_stmt', [
    ('CREATE TABLE public.garbage (id SERIAL)', 'DROP TABLE public.garbage CASCADE'),
    ('CREATE SCHEMA garbage', 'DROP SCHEMA garbage CASCADE'),
    ('CREATE EXTENSION pgcrypto', 'DROP EXTENSION pgcrypto'),
])
def test_dirty_database_table(transacted_postgresql_db, create_stmt, drop_stmt):
    """Verify an exception is thrown when the database isn't cleaned up with
    just a rollback."""
    transacted_postgresql_db.connection.execute(create_stmt + '; COMMIT;')

    with pytest.raises(errors.DatabaseIsDirtyError):
        transacted_postgresql_db.reset_db()

    transacted_postgresql_db.connection.execute(drop_stmt + '; COMMIT;')


@pytest.mark.parametrize('db_class', [
    pytest_pgsql.database.TransactedPostgreSQLTestDB,
    pytest_pgsql.database.PostgreSQLTestDB,
])
def test_restore_no_snapshot_transacted_fails(transacted_postgresql_db, db_class):
    """Blow up if the user tries restoring the database without a snapshot."""
    db = db_class(transacted_postgresql_db.postgresql_url,
                  transacted_postgresql_db.connection)
    with pytest.raises(errors.NoSnapshotAvailableError):
        db.restore_to_snapshot()


@pytest.mark.parametrize('db_class', [
    pytest_pgsql.database.TransactedPostgreSQLTestDB,
    pytest_pgsql.database.PostgreSQLTestDB,
])
def test_reset_db_no_snapshot_is_ok(transacted_postgresql_db, db_class, mocker):
    """Resetting without a snapshot should skip restore_to_snapshot()."""
    db = db_class(transacted_postgresql_db.postgresql_url,
                  transacted_postgresql_db.connection)

    restore_mock = mocker.patch.object(db, 'restore_to_snapshot')
    db.reset_db()
    assert restore_mock.call_count == 0
