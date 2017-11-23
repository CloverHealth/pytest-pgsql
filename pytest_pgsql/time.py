"""A utility class for freezing timestamps inside SQL queries."""

import datetime
import functools
import re

import freezegun
import sqlalchemy.event as sa_event
import sqlalchemy.orm.session as sqla_session


_TIMESTAMP_REPLACEMENT_FORMATS = (
    # Functions
    (r'\b((NOW|CLOCK_TIMESTAMP|STATEMENT_TIMESTAMP|TRANSACTION_TIMESTAMP)\s*\(\s*\))',
     r"'{:%Y-%m-%d %H:%M:%S.%f %z}'::TIMESTAMPTZ"),
    (r'\b(TIMEOFDAY\s*\(\s*\))', r"'{:%Y-%m-%d %H:%M:%S.%f %z}'::TEXT"),

    # Keywords
    (r'\b(CURRENT_DATE)\b', r"'{:%Y-%m-%d}'::DATE"),
    (r'\b(CURRENT_TIME)\b', r"'{:%H:%M:%S.%f %z}'::TIMETZ"),
    (r'\b(CURRENT_TIMESTAMP)\b', r"'{:%Y-%m-%d %H:%M:%S.%f %z}'::TIMESTAMPTZ"),
    (r'\b(LOCALTIME)\b', r"'{:%H:%M:%S.%f}'::TIME"),
    (r'\b(LOCALTIMESTAMP)\b', r"'{:%Y-%m-%d %H:%M:%S.%f}'::TIMESTAMP"),
)


class SQLAlchemyFreezegun(object):
    """Freeze timestamps in all SQL executed while this freezegun is active.

    This works by hooking into SQLAlchemy's "before_cursor_execute" event and
    modifying the query to use a predetermined date/time/timestamp instead of
    calling ``NOW()`` or ``CURRENT_TIMESTAMP``. This gives reasonable assurance
    that all timestamps in database queries are predictable.

    You can use this as a context manager or by manually invoking functions::

        def test_foo(postgresql_db):
            postgresql_db.time.freeze('2017-01-01 00:00:00')
            ...
            postgresql_db.time.unfreeze()


        def test_bar(postgresql_db):
            with postgresql_db.time.freeze('2017-01-01 00:00:00'):
                ...

    You also might be interested in the :func:`freeze_time` decorator.

    .. note ::
        Because this scans each query multiple times with a regular expression
        it can hurt performance considerably. You probably won't want to use
        this unless it's necessary.

    .. warning ::
        Because this works by modifying the query with regular expressions, it
        *is* fallible and unexpected behavior such as inexplicable syntax errors
        can occur. Some known cases in which it'll fail:

        **Triggers**

        Columns with an ``ON UPDATE CURRENT_TIMESTAMP`` trigger attached to them
        will always be set to the real time on an update, unless you override it
        in the insert statement.

        **Stored Procedures**

        Since only the query is modified, stored procedures will still use the
        real current time.

        **Strings**

        While rare and definitely ill-advised, when a query uses a keyword in a
        string constant it will still get replaced. For example, this:

        .. code-block:: sql

            SELECT CURRENT_DATE AS "current_date"

        will become this:

        .. code-block:: sql

            SELECT '2017-04-05'::DATE AS "'2017-04-05'::DATE"

    Arguments:
        connectable:
            A :class:`~sqlalchemy.engine.Connection`,
            :class:`~sqlalchemy.engine.Engine`, or a bound
            :class:`~sqlalchemy.orm.session.Session`. Only queries executed with
            this object will be modified.
    """
    def __init__(self, connectable):
        # If the caller gives us a session, take the underlying connection or
        # engine instead.
        if isinstance(connectable, sqla_session.Session):
            if connectable.bind is None:
                raise TypeError("Can't use unbound `Session` object for freezing.")
            connectable = connectable.bind

        self._connectable = connectable
        self._query_hook = None
        self._freeze_time = None
        self._freezer_factory = None

    @property
    def is_frozen(self):
        """Is time currently being frozen?"""
        return self._freezer_factory is not None

    @property
    def freezer(self):
        """Return the currently active ``FrozenDateTimeFactory`` instance, or
        ``None`` if time is not being frozen."""
        return self._freezer_factory

    def freeze(self, when=None, **freezegun_kwargs):
        """Start modifying timestamps in queries and Python code.

        Arguments:
            when (str|date|time|datetime):
                The point in time to freeze all date and time functions to. This
                will affect both PostgreSQL and Python. If a string is given, it
                must be a date and/or time in a format that Postgres recognizes.

                If not given, defaults to the current timestamp in UTC.

            **freezegun_kwargs:
                Any additional arguments to pass to ``freezegun.freeze_time()``.

        .. note ::
            If ``when`` is a `naive datetime <https://docs.python.org/3/library/datetime.html>`_,
            the default timezone is UTC, *not* the local timezone.
        """
        if not when:
            when = datetime.datetime.now(datetime.timezone.utc)

        self.unfreeze()
        self._freeze_time = freezegun.freeze_time(when, **freezegun_kwargs)
        self._freezer_factory = self._freeze_time.start()

        # pylint: disable=unused-argument
        @sa_event.listens_for(self._connectable, 'before_cursor_execute',
                              retval=True)
        def _hook(conn, cursor, statement, parameters, context, executemany):
            """Query hook to modify all timestamps."""
            # We use datetime.now() here because it should already be frozen. No
            # need to hardcode it.
            timestamp = datetime.datetime.now(datetime.timezone.utc)

            for regex, replacement in _TIMESTAMP_REPLACEMENT_FORMATS:
                statement = re.sub(regex, replacement.format(timestamp),
                                   statement, flags=re.IGNORECASE)

            return statement, parameters
        # pylint: enable=unused-argument

        # Set up our query modifier to listen for execution events
        self._query_hook = _hook
        sa_event.listen(self._connectable, 'before_cursor_execute', _hook)

        # This is correct. Do *not* change this to return self._freezer_factory
        # or you will break the context manager behavior.
        return self

    def unfreeze(self):
        """Stop modifying timestamps in queries."""
        if self._query_hook:
            sa_event.remove(self._connectable, 'before_cursor_execute',
                            self._query_hook)
            self._query_hook = None

        if self._freeze_time is not None:
            self._freeze_time.stop()
            self._freeze_time = None
            self._freezer_factory = None

    def __enter__(self):
        """Start the time freeze when this is used as a context manager."""
        self.freeze()
        return self._freezer_factory

    def __exit__(self, *error_args):
        """Exiting the context manager, stop freezing time."""
        self.unfreeze()


def _is_freezeable(obj):
    """Determine if obj has the same freezing interface as `PostgreSQLTestUtil`.

    For some reason isinstance doesn't work properly with fixtures, so checking
    ``isinstance(obj, PostgreSQLTestDB)`` will always fail. Instead, we check to
    see if obj.time.freeze()/unfreeze() are present, and that the `time` member
    has context manager behavior implemented.
    """
    return (
        hasattr(obj, 'time') and
        callable(getattr(obj.time, 'freeze', None)) and
        callable(getattr(obj.time, 'unfreeze', None)) and
        callable(getattr(obj.time, '__enter__', None)) and
        callable(getattr(obj.time, '__exit__', None))
    )


def freeze_time(when):
    """Freeze time inside a test, including in queries made to the database.

    This differs from normal ``freezegun`` usage in that it also works for
    database queries, with some caveats (see `SQLAlchemyFreezegun`).

    The test modified by this decorator must use one and only one fixture that
    returns a `PostgreSQLTestDB` instance. This means you can't *implicitly* use
    a fixture with the ``pytest.mark.usefixtures`` decorator.

    Sample usage::

        @pytest_pgsql.freeze_time('2999-12-31')
        def test_baz(postgresql_db):
            assert datetime.date.today() == datetime.date(2999, 12, 31)

            now = postgresql_db.engine.execute('SELECT CURRENT_DATE').scalar()
            assert now == datetime.date(2999, 12, 31)

    Arguments:
        when (str|date|time|datetime):
            The timestamp to freeze all date and time functions to. This will
            affect both PostgreSQL and Python.
    """
    def decorator(func):
        @functools.wraps(func)
        def test_function_wrapper(*args, **kwargs):
            # Get all fixtures passed to the test function; one and only one of
            # these must be a freezable database.
            databases = [a for a in args if _is_freezeable(a)]
            databases.extend(v for v in kwargs.values() if _is_freezeable(v))

            if len(databases) != 1:
                func_name = getattr(func, '__name__', type(func).__name__)
                raise RuntimeError(
                    'You must use exactly *one* database fixture with the '
                    '`freeze_time` decorator. %r has %d.'
                    % (func_name, len(databases)))

            with databases[0].time.freeze(when):
                return func(*args, **kwargs)
        return test_function_wrapper
    return decorator
