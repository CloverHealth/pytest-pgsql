"""Tests for the freezegun adaptation for the fixtures."""

import datetime

import pytest_pgsql
import pytest
import sqlalchemy.orm as sqla_orm


_PGFREEZE_DATETIME_TZ = datetime.datetime(2099, 12, 31, 23, 59, 59, 123000,
                                          datetime.timezone.utc)
_PGFREEZE_TIME_TZ = _PGFREEZE_DATETIME_TZ.timetz()
_PGFREEZE_DATE = _PGFREEZE_DATETIME_TZ.date()
_PGFREEZE_DATETIME_NAIVE = _PGFREEZE_DATETIME_TZ.replace(tzinfo=None)
_PGFREEZE_TIME_NAIVE = _PGFREEZE_TIME_TZ.replace(tzinfo=None)

# These are identical to the above timestamps except they're rounded down to the
# nearest second.
_PGFREEZE_DATETIME_TZ_SECS = _PGFREEZE_DATETIME_TZ.replace(microsecond=0)
_PGFREEZE_TIME_TZ_SECS = _PGFREEZE_TIME_TZ.replace(microsecond=0)
_PGFREEZE_DATETIME_NAIVE_SECS = _PGFREEZE_DATETIME_NAIVE.replace(microsecond=0)
_PGFREEZE_TIME_NAIVE_SECS = _PGFREEZE_TIME_NAIVE.replace(microsecond=0)


@pytest.mark.parametrize('expression', [
    'SELECT %s',
    "SELECT date_trunc('MICROSECONDS', %s)",
])
@pytest.mark.parametrize('function,expected', [
    ('CURRENT_TIMESTAMP', _PGFREEZE_DATETIME_TZ),
    ('NOW()', _PGFREEZE_DATETIME_TZ),
    ('now (  )', _PGFREEZE_DATETIME_TZ),
    ('transaction_timestamp()', _PGFREEZE_DATETIME_TZ),
    ('StaTemEnt_TimeStaMp()', _PGFREEZE_DATETIME_TZ),
    ('LocalTimestamp', _PGFREEZE_DATETIME_NAIVE),

    # These ensure we can form syntactically valid statements with casts.
    ('current_timestamp::timestamp', _PGFREEZE_DATETIME_NAIVE),
    ('localtimestamp::timestamp(0)', _PGFREEZE_DATETIME_NAIVE_SECS),
    ('current_timestamp(3)::timestamptz(0)', _PGFREEZE_DATETIME_TZ_SECS),
])
def test_basic_timestamp(clean_db, expression, function, expected):
    """Basic test to see if the database freezegun works."""
    clean_db.time.freeze(_PGFREEZE_DATETIME_TZ)
    assert clean_db.time.freezer is not None
    assert clean_db.time.is_frozen

    query = expression % function

    # Test first with the connectible...
    result = clean_db._conn.execute(query).scalar()   # pylint: disable=protected-access
    assert result == expected, 'Freezing the connection failed.'

    # ...then with the session.
    result = clean_db.session.execute(query).scalar()
    assert result == expected, 'Freezing the session failed.'

    clean_db.time.unfreeze()
    assert clean_db.time.freezer is None
    assert not clean_db.time.is_frozen


@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
@pytest.mark.parametrize('function,expected', [
    ('CURRENT_TIMESTAMP(0)', _PGFREEZE_DATETIME_TZ_SECS),
    ('CURRENT_TIME (0) ', _PGFREEZE_TIME_TZ_SECS),
    ('localtime(0   )', _PGFREEZE_TIME_NAIVE_SECS),
    ('LocalTimestamp ( 0 )', _PGFREEZE_DATETIME_NAIVE_SECS),
    ('NOW()::TIMESTAMPTZ(0)', _PGFREEZE_DATETIME_TZ_SECS),
])
def test_precision_override(clean_db, function, expected, conn_name):
    """Verify we form valid datetimes even when using precision truncation."""
    conn = getattr(clean_db, conn_name)
    with clean_db.time.freeze(_PGFREEZE_DATETIME_TZ):
        result = conn.execute('SELECT ' + function).scalar()
        assert result == expected, 'Freezing the engine failed.'


@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
def test_timeofday(clean_db, conn_name):
    """``timeofday()`` is an oddball - it returns a string, not a timestamp."""
    conn = getattr(clean_db, conn_name)
    with clean_db.time.freeze(_PGFREEZE_DATETIME_TZ):
        result = conn.execute('SELECT TIMEOFDAY() -- :)').scalar()
        assert result == '2099-12-31 23:59:59.123000 +0000'


@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
@pytest.mark.parametrize('expression,expected', [
    # The query modifier shouldn't replace the 'CURRENT_DATE' part of this.
    ("SELECT 'CURRENT_DATETIME'::TEXT", 'CURRENT_DATETIME'),
])
def test_ignore_id(clean_db, conn_name, expression, expected):
    """These expressions should not be modified."""
    conn = getattr(clean_db, conn_name)
    result = conn.execute(expression).scalar()
    assert result == expected


@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
def test_context(clean_db, conn_name):
    """Ensure that time resumes normally after the context manager exits."""
    expected_ts = datetime.datetime(2525, 1, 1, 0, 0, 0,
                                    tzinfo=datetime.timezone.utc)

    conn = getattr(clean_db, conn_name)
    with clean_db.time.freeze(expected_ts):
        result = conn.execute('SELECT NOW()').scalar()
        assert result == expected_ts
        assert clean_db.time.freezer is not None
        assert clean_db.time.is_frozen

    assert clean_db.time.freezer is None
    assert not clean_db.time.is_frozen
    assert datetime.datetime.now(datetime.timezone.utc) != expected_ts
    result = conn.execute('SELECT NOW()').scalar()
    assert result != expected_ts


def test_unfreeze_twice(clean_db):
    """Attempting to unfreeze when not frozen shouldn't throw exceptions.

    This verifies that the query execution hook is properly removed and won't
    cause an exception due to the freezer and hook being nulled out.
    """
    clean_db.time.unfreeze()
    assert clean_db.time.freezer is None

    with clean_db.time.freeze('1900-01-01'):
        date = clean_db.session.execute('SELECT CURRENT_DATE').scalar()
        assert date == datetime.date(1900, 1, 1)
        assert clean_db.time.freezer is not None

    # Shouldn't be frozen anymore, but let's try unfreezing anyway.
    clean_db.time.unfreeze()
    assert clean_db.time.freezer is None


def test_tick(clean_db):
    """Verify we can use the freezer factory thing."""
    expected = datetime.datetime(2016, 12, 31, 23, 59, 59)
    with clean_db.time.freeze(expected) as freezer:
        db_now = clean_db.session.execute('SELECT LOCALTIMESTAMP').scalar()
        assert datetime.datetime.now() == expected
        assert db_now == expected

        freezer.tick()

        expected = datetime.datetime(2017, 1, 1, 0, 0, 0)
        db_now = clean_db.session.execute('SELECT LOCALTIMESTAMP').scalar()
        assert datetime.datetime.now() == expected
        assert db_now == expected


@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
def test_change_time_twice(clean_db, conn_name):
    """Ensure we can change the time multiple times in a row."""
    expected_ts = datetime.datetime(1066, 10, 14, 9, 0, 0,
                                    tzinfo=datetime.timezone.utc)

    conn = getattr(clean_db, conn_name)
    with clean_db.time.freeze(expected_ts):
        result = conn.execute('SELECT NOW()').scalar()
        assert result == expected_ts
        assert clean_db.time.freezer is not None
        assert clean_db.time.is_frozen

    # Time should be back to normal for the time being
    result = conn.execute('SELECT NOW()').scalar()
    assert result != expected_ts
    assert clean_db.time.freezer is None

    expected_ts = datetime.datetime(1234, 5, 6, 7, 8, 9,
                                    tzinfo=datetime.timezone.utc)
    with clean_db.time.freeze(expected_ts):
        result = conn.execute('SELECT NOW()').scalar()
        assert result == expected_ts
        assert clean_db.time.freezer is not None
        assert clean_db.time.is_frozen

    assert clean_db.time.freezer is None
    assert not clean_db.time.is_frozen

    # Make sure time is unfrozen again.
    result = conn.execute('SELECT NOW()').scalar()
    assert result != expected_ts


@pytest_pgsql.freeze_time('1111-11-11 11:11:11.111000 +0000')
@pytest.mark.parametrize('conn_name', [
    '_conn',
    'session',
])
def test_decorator(clean_db, conn_name):
    """We're using the freezing decorator, all time should be frozen in here."""
    expected_time = datetime.datetime(1111, 11, 11, 11, 11, 11, 111000,
                                      datetime.timezone.utc)

    # First make sure that we activated freezegun properly
    now = datetime.datetime.now(datetime.timezone.utc)
    assert now == expected_time, 'Freezegun not activated!'

    conn = getattr(clean_db, conn_name)

    # Freezegun is active, now make sure that the database works as expected.
    now = conn.execute('SELECT NOW()').scalar()
    assert now == expected_time, "Database time isn't frozen."


def test_decorator_no_fixtures():
    """The decorator should crash if there are no database fixtures."""
    with pytest.raises(RuntimeError) as excinfo:
        @pytest_pgsql.freeze_time('1999-12-31')
        def bad_test():
            """This test doesn't use a database fixture!"""

        bad_test()

    assert str(excinfo.value).endswith("'bad_test' has 0.")


def test_decorator_too_many_fixtures(transacted_postgresql_db, postgresql_db):
    """The decorator should crash if there's more than one database fixture."""
    with pytest.raises(RuntimeError) as excinfo:
        @pytest_pgsql.freeze_time('1999-12-31')
        def bad_test(fixture_a, fixture_b):
            """This test uses too many database fixtures!"""

        bad_test(transacted_postgresql_db, postgresql_db)

    assert str(excinfo.value).endswith("'bad_test' has 2.")


def test_can_use_session_transacted(clean_db):
    """Ensure we can use a Session for freezing time in the transacted DB."""
    freezer = pytest_pgsql.SQLAlchemyFreezegun(clean_db.session)
    with freezer.freeze('2000-01-01'):
        expected = datetime.date(2000, 1, 1)
        assert datetime.date.today() == expected

        now = clean_db.session.execute('SELECT CURRENT_DATE').scalar()
        assert now == expected

        # pylint: disable=protected-access
        now = clean_db._conn.execute('SELECT CURRENT_DATE').scalar()
        # pylint: enable=protected-access
        assert now == expected


def test_unbound_session_crashes():
    """Ensure attempting to use an unbound session will fail."""
    sessionmaker = sqla_orm.sessionmaker()
    session = sessionmaker()

    with pytest.raises(TypeError):
        pytest_pgsql.SQLAlchemyFreezegun(session)
