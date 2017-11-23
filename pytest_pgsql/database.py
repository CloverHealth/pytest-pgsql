import abc
import csv

import pytest
import sqlalchemy as sqla
import sqlalchemy.exc as sqla_exc
import sqlalchemy.orm as sqla_orm

import pytest_pgsql.time
from pytest_pgsql import errors


#: A query to get a snapshot of all existing tables and views and their OIDs.
TABLE_SNAPSHOT_QUERY = """
SELECT
  schemaname AS schema_name,
  tablename AS table_name,
  (schemaname || '.' || tablename)::regclass::oid AS table_oid
FROM pg_tables
"""


def create_database_snapshot(connectable):
    """Create a snapshot of the current state of the database so that we can
    restore it to this state when the test exits.

    Arguments:
        connectable (`sqlalchemy.engine.Connectable`):
            The engine, connection, or other Connectable to use to take this
            snapshot.

    Returns (dict):
        A dictionary with three keys:

        * ``schemas``: A tuple of the names of all the schemas present.
        * ``tables``: A list of all of the tables present. Each item in the list
          is a dictionary with the schema name, table name, and table OID.
        * ``extensions``: A tuple of the names of all the extensions currently
          installed.
    """
    execute = connectable.execute
    return {
        'schemas': tuple(r['nspname'] for r in execute('SELECT nspname FROM pg_namespace')),
        'tables': [dict(r) for r in execute(TABLE_SNAPSHOT_QUERY)],
        'extensions': tuple(
            r['extname'] for r in execute('SELECT extname FROM pg_extension')
        )
    }


class PostgreSQLTestDBBase(metaclass=abc.ABCMeta):
    """Utility to wrap ``testing.postgresql`` and provide extra functionality.

    This is a base class and cannot be instantiated directly. Take a look at the
    main subclasses, `PostgreSQLTestDB` and `TransactedPostgreSQLTestDB`.

    Arguments:
        url (str):
            The connection URI of the PostgreSQL test database.

        connectable (`sqlalchemy.engine.Connectable`):
            The SQLAlchemy engine or connection to be used for accessing the
            test database. The ORM session will be backed by this connectable.

        restore_state (dict):
            Optional. A snapshot of the state to restore the database to after
            each test. If not given, the only cleanup that can be performed is a
            rollback of the current transaction.

            .. seealso:: `create_database_snapshot`
    """
    def __init__(self, url, connectable, restore_state=None):
        self._conn = connectable
        self._sessionmaker = sqla_orm.sessionmaker(bind=connectable)
        self.session = self._sessionmaker()
        self.postgresql_url = url
        self.time = pytest_pgsql.time.SQLAlchemyFreezegun(connectable)
        self._restore_state = restore_state

    def is_dirty(self):
        """Determine if there are tables, schemas, or extensions installed that
        weren't there when the test started.

        If this returns ``True``, then a full teardown of the database is needed
        to return it to its original state.

        Returns (bool):
            ``True`` if the database needs to be cleaned up with `reset_db`,
            ``False`` otherwise.
        """
        original_table_oids = tuple(t['table_oid'] for t in self._restore_state['tables'])

        query = sqla.text("""
            SELECT
              EXISTS(
                SELECT 1 FROM pg_namespace
                WHERE nspname NOT IN :ignore_schemas
                LIMIT 1
              )
              OR
              EXISTS(
                SELECT 1 FROM pg_tables
                WHERE (schemaname || '.' || tablename)::regclass::oid NOT IN :ignore_tables
                LIMIT 1
                -- Checking for OIDs in our snapshot that're missing from pg_tables
                -- will give us a list of all preexisting tables that are now
                -- missing. Do we care?
              )
              OR
              EXISTS(
                SELECT 1 FROM pg_extension
                WHERE extname NOT IN :ignore_extensions
                LIMIT 1
              )
        """).bindparams(ignore_tables=original_table_oids,
                        ignore_schemas=self._restore_state['schemas'],
                        ignore_extensions=self._restore_state['extensions'])

        return self._conn.execute(query).scalar()

    def _clean_up_extensions(self):
        """Drop any extensions installed by the test."""
        # Build a list of all extensions we installed during the tests and drop
        # them.
        new_extensions = self._conn.execute(sqla.text("""
            SELECT extname FROM pg_extension
            WHERE extname NOT IN :ignore
        """).bindparams(ignore=self._restore_state['extensions']))

        quote = self.id_quoter.quote
        drop_query = ';'.join(
            'DROP EXTENSION IF EXISTS %s CASCADE' % quote(r['extname'])
            for r in new_extensions)
        if drop_query:
            self._conn.execute(drop_query)

    def _clean_up_schemas(self):
        """Drop all schemas created during this test.

        .. warning::

            This should NOT be executed before we're sure that all preexisting
            tables are back in their original schemas. No checks are performed
            to ensure that preexisting tables we need to save aren't in any of
            the schemas we're about to drop.
        """
        execute = self._conn.execute
        quote = self.id_quoter.quote

        extra_schemas = execute(sqla.text("""
            SELECT
              nspname
            FROM pg_namespace
            WHERE
              nspname != 'pytest_pgsql'
              AND nspname NOT IN :schemas
        """).bindparams(schemas=self._restore_state['schemas']))

        for schema in (r['nspname'] for r in extra_schemas):
            try:
                execute('DROP SCHEMA %s CASCADE' % quote(schema))
            except sqla_exc.OperationalError:   # pragma: no cover
                # Sometimes when we drop really large schemas the database will
                # crash because it runs out of memory. If that happens we gotta
                # drop all the tables in the schema one by one.

                # Recover from the exception.
                self.rollback()

                extra_tables = execute(
                    sqla.text("""
                        SELECT
                          table_name
                        FROM pytest_pgsql.current_tables
                        WHERE schema_name = :name
                    """)
                    .bindparams(name=schema))

                for table in (r['table_name'] for r in extra_tables):
                    execute('DROP TABLE %s.%s CASCADE'
                            % (quote(schema), quote(table)))

                execute('DROP SCHEMA %s CASCADE' % quote(schema))

    def _undo_table_renames(self):
        """Undo table renames and ensure preexisting tables are in their original
        schemas.
        """
        execute = self._conn.execute
        quote = self.id_quoter.quote

        # We can't just rename them one by one because if two tables swapped
        # names that'd cause a collision. Instead, we rename each table to use
        # its schema and table OIDs so that the names are guaranteed(ish) to be
        # unique, *then* move everything back to where it was.

        # Build a list of all original tables that have been renamed or changed
        # schemas.
        rows = execute("""
            SELECT
              cur.*,
              orig.schema_name AS orig_schema,
              orig.table_name AS orig_table,
              floor(random() * 1000) AS rnd_i   -- See explanation below
            FROM pytest_pgsql.original_tables AS orig
            -- Use LEFT JOIN so table_oid will be null if a table was deleted.
            LEFT JOIN pytest_pgsql.current_tables AS cur
              ON orig.table_oid = cur.table_oid
            WHERE (
              orig.table_name != cur.table_name
              OR orig.schema_name != cur.schema_name
              OR cur.table_oid IS NULL
            )
            AND cur.schema_name != 'pytest_pgsql'
        """)
        moved_tables = [dict(r) for r in rows]

        # Detect original tables that were deleted, and crash if any were.
        deleted_tables = [t for t in moved_tables if t['table_oid'] is None]
        if deleted_tables:  # pragma: no cover
            raise errors.DatabaseRestoreFailedError(
                "Can't restore dropped table(s): " +
                ', '.join('{orig_schema}.{orig_table}'.format_map(t)
                          for t in deleted_tables))

        # Rename each table to something unique-ish - a combination of the
        # table's OID and a random number. Now, when we start moving/renaming
        # tables back to what they used to be, the chances of a collision are
        # minimal.
        rename_query = ';'.join(
            'ALTER TABLE %s.%s RENAME TO %s' % (
                quote(t['schema_name']),
                quote(t['table_name']),
                '_pgtu_{orig_table_oid}{rnd_i}'.format_map(t)
            )
            for t in moved_tables)
        if rename_query:    # pragma: no cover
            execute(rename_query)

        # All tables renamed, start moving them back to their original places.
        move_query = ';'.join("""
            ALTER TABLE {cur_schema}.{rntable} RENAME TO {orig_table};
            CREATE SCHEMA IF NOT EXISTS {orig_schema};
            ALTER TABLE {cur_schema}.{orig_table} SET SCHEMA {orig_schema};
        """.format(cur_schema=quote(t['schema_name']),
                   rntable='_pgtu_{orig_table_oid}{rnd_i}'.format_map(t),
                   orig_table=quote(t['orig_table']),
                   orig_schema=quote(t['orig_schema']))
                              for t in moved_tables)
        if move_query:  # pragma: no cover
            execute(move_query)

    def _clean_up_tables(self):
        """Drop any tables created by the test.

        This should be executed *after* extra schemas were dropped to minimize
        the number of tables that have to be dropped individually.
        """
        execute = self._conn.execute
        quote = self.id_quoter.quote

        ignored_tables = tuple(
            '{schema_name}.{table_name}'.format_map(t)
            for t in self._restore_state['tables']
        )

        new_tables = execute(sqla.text("""
            SELECT
              schemaname,
              tablename
            FROM pg_tables
            WHERE schemaname || '.' || tablename NOT IN :ignore;
        """).bindparams(ignore=ignored_tables))

        drop_query = ';'.join(
            'DROP TABLE %s.%s CASCADE' % (quote(r['schemaname']), quote(r['tablename']))
            for r in new_tables
        )
        if drop_query:  # pragma: no cover
            execute(drop_query)

    def restore_to_snapshot(self):
        """Restore the database to its original state.

        :raises `NoSnapshotAvailableError`:
            If the restore snapshot wasn't given to the constructor.
        """
        if not self._restore_state:
            raise errors.NoSnapshotAvailableError()

        self._clean_up_extensions()

        self._conn.execute("""
            DROP SCHEMA IF EXISTS pytest_pgsql CASCADE;
            CREATE SCHEMA pytest_pgsql;

            CREATE UNLOGGED TABLE pytest_pgsql.current_tables AS {table_query};
            CREATE UNLOGGED TABLE pytest_pgsql.original_tables (
              LIKE pytest_pgsql.current_tables EXCLUDING ALL
            );
        """.format(table_query=TABLE_SNAPSHOT_QUERY))

        orig_tables = self.get_table('pytest_pgsql.original_tables')
        self._conn.execute(orig_tables.insert().values(self._restore_state['tables']))

        self._undo_table_renames()
        self._clean_up_schemas()
        self._clean_up_tables()
        self._conn.execute('DROP SCHEMA pytest_pgsql CASCADE; COMMIT;')

    def reset_db(self):
        """Reset the database to its initial state."""
        self.time.unfreeze()
        self.rollback()
        if self._restore_state is not None:
            self.restore_to_snapshot()

    @property
    def id_quoter(self):
        """An :class:`~sqlalchemy.sql.compiler.IdentifierPreparer` you can use
        to quote table names, identifiers, etc. to prevent SQL injection
        vulnerabilities.
        """
        return self._conn.dialect.preparer(self._conn.dialect)

    def is_extension_available(self, name):
        """Determine if the named extension is available for installation.

        Arguments:
            name (str):
                The name of the extension to search for.

        Returns (bool):
            ``True`` if the extension is available, ``False`` otherwise. Note
            that availability is no guarantee the extension will install
            successfully.
        """
        query = sqla.text(
            'SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name=:n LIMIT 1)'
        ).bindparams(n=name)
        return self._conn.execute(query).scalar()

    def install_extension(self, extension, if_available=False, exists_ok=False,
                          schema=None):
        """Install a PostgreSQL extension.

        Arguments:
            extension (str):
                The name of the extension to install.

            schema (str):
                Optional. The name of the schema to install the extension into.
                If not given, it'll be installed in the default schema. Consult
                the PostgreSQL docs for `CREATE EXTENSION`__ for more info.

            if_available (bool):
                Only attempt to install the extension if the PostgreSQL server
                supports it.

            exists_ok (bool):
                Don't bother installing the extension if it's already installed.

        Returns (bool):
            ``True`` if the extension was installed. If ``if_available`` is set,
            then this returns ``False`` if installation was skipped because the
            extension isn't available.

        .. note::
            Dependencies are *not* automatically installed.

        .. _pg_doc: https://www.postgresql.org/docs/current/static/sql-createextension.html
        __ pg_doc_
        """
        if if_available and not self.is_extension_available(extension):
            return False

        check = 'IF NOT EXISTS' if exists_ok else ''

        if schema:
            stmt = 'CREATE EXTENSION {check} {ext} WITH SCHEMA {schema}'.format(
                check=check,
                ext=self.id_quoter.quote_identifier(extension),
                schema=self.id_quoter.quote_schema(schema))
        else:
            stmt = 'CREATE EXTENSION {check} {ext}'.format(
                check=check,
                ext=self.id_quoter.quote_identifier(extension))

        self._conn.execute(stmt)
        return True

    def has_extension(self, extension):
        """Determine if the given extension has already been installed.

        Arguments:
            extension (str):
                The name of the extension to search for.

        Returns (bool):
            ``True`` if the extension is installed, ``False`` otherwise.

        .. note ::
            This is *not* the same as checking the availability of an extension.
            You'll need to use `is_extension_available` for that.
        """
        query = sqla.text(
            'SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname=:n LIMIT 1)'
        ).bindparams(n=extension)
        return self._conn.execute(query).scalar()

    def has_schema(self, schema):
        """Determine if the given schema exists in the database.

        Arguments:
            schema (str):
                The name of the schema to check for.

        Returns (bool):
            ``True`` if the schema exists, ``False`` otherwise.
        """
        query = sqla.text(
            'SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=:s LIMIT 1)'
        ).bindparams(s=schema)
        return self._conn.execute(query).scalar()

    def has_table(self, table):
        """Determine if the given table exists in the database.

        ``table`` must reference a regular table, not a view, foreign table, or
        temporary table.

        Arguments:
            table:
                The table to search for. This can be any of the following:

                 - A full table name with the schema: ``myschema.mytable``.
                 - Just the table name: ``mytable``. This will search in *all*
                   schemas for a table with the given name, *not* the search
                   path.
                 - A `sqlalchemy.schema.Table` object.
                 - A SQLAlchemy ORM declarative model.

        Returns (bool):
            ``True`` if the table exists, ``False`` otherwise.
        """
        if isinstance(table, str):
            if '.' in table:
                schema_name, _sep, table_name = table.partition('.')
            else:
                schema_name = ''
                table_name = table
        elif isinstance(table, sqla.Table):
            schema_name = table.schema
            table_name = table.name
        elif hasattr(table, '__table__'):
            # Assume this is an ORM declarative model.
            schema_name = table.__table__.schema
            table_name = table.__table__.name
        else:
            raise TypeError(
                'Expected str, SQLAlchemy Table, or declarative model, got %r.'
                % type(table).__name__)

        subquery = 'SELECT 1 FROM pg_tables WHERE tablename = :t'
        params = {'t': table_name}

        if schema_name:
            subquery += ' AND schemaname = :s'
            params['s'] = schema_name

        query = sqla.text('SELECT EXISTS (' + subquery + ' LIMIT 1)').bindparams(**params)
        return self._conn.execute(query).scalar()

    def create_schema(self, *schemas, exists_ok=False):
        """Create one or more schemas in the test database.

        Schemas are created in a single operation.

        Arguments:
            *schemas (str):
                The names of the schemas to create.

            exists_ok (bool):
                Don't throw an exception if the schema exists already.
        """
        check = 'IF NOT EXISTS' if exists_ok else ''
        quoted_names = [self.id_quoter.quote_schema(s) for s in schemas]
        query = ';'.join('CREATE SCHEMA %s %s' % (check, s) for s in quoted_names)
        return self._conn.execute(query)

    def create_table(self, *tables):
        """Create a table in the database.

        If the table is in a schema and that schema does not exist, it will be
        created.

        Arguments:
            *tables:
                `sqlalchemy.schema.Table` instances or declarative model
                classes.
        """
        for table in tables:
            if not isinstance(table, sqla.Table):
                table = table.__table__

            if table.schema is not None:
                self.create_schema(table.schema, exists_ok=True)
            table.create(self._conn)

    def get_table(self, table, metadata=None):
        """Create a `sqlalchemy.schema.Table` instance from an existing table in
        the database.

        SQLAlchemy refers to this as `reflection
        <http://docs.sqlalchemy.org/en/latest/core/reflection.html>`_.

        Arguments:
            table (str):
               The name of the table to reflect, including the schema name if
               applicable (e.g. ``'my_schema.the_table'``).

            metadata (`sqlalchemy.schema.MetaData`):
                The metadata to associate the table with. If not given, a new
                :class:`~sqlalchemy.schema.MetaData` object will be created and
                bound to the current connection or engine.

        Returns (`sqlalchemy.schema.Table`):
            The reflected table.
        """
        if not metadata:
            metadata = sqla.MetaData(bind=self._conn)

        # If the metadata isn't bound to an engine or connection we need to pass
        # `autoload_with` and a Connectible.
        if not metadata.bind:
            kwargs = {'autoload_with': self._conn}
        else:
            kwargs = {}

        schema_name, _sep, table_name = table.partition('.')
        if table_name:
            # Caller passed in a table and a schema.
            return sqla.Table(table_name, metadata, autoload=True,
                              schema=schema_name, **kwargs)

        # Caller passed in a table name without a schema. The name of the table
        # will be in `schema_name` due to how `partition()` works.
        return sqla.Table(schema_name, metadata, autoload=True, **kwargs)

    def run_sql_file(self, source, **bindings):
        """Convenience method for running a SQL file, optionally filling in any
        bindings present in the file.

        If the ``bindings`` mapping is empty, the query is executed exactly as
        is in the file. If ``bindings`` contains values, the query text is
        wrapped inside a :class:`~sqlalchemy.sql.expression.TextClause` with a
        call to :func:`sqlalchemy.expression.text` before execution. As such,
        the only supported parametrization syntax is the one that uses colons,
        e.g.:

        .. code-block:: sql

            DELETE FROM users WHERE username = :user

        Arguments:
            source:
                The path to the SQL file to run, or a file-like object with a
                ``read()`` function.

            **bindings:
                Values to bind to the query once the file is loaded. If no
                values are given, no binding will be performed and the file will
                be executed exactly as is.

        Returns (`sqlalchemy.engine.ResultProxy`):
            The results of the SQL file's execution.
        """
        if isinstance(source, str):
            with open(source, 'r') as fd:
                to_run = fd.read()
        else:
            to_run = source.read()

        if bindings:
            return self._conn.execute(sqla.text(to_run).bindparams(**bindings))
        return self._conn.execute(sqla.text(to_run))

    def load_csv(self, csv_source, table, dialect='excel', truncate=False,
                 cascade=False):
        """Load an existing table with the contents of a CSV.

        Arguments:
            csv_source:
                The path to a CSV file, or a readable file-like object.

            table:
                The name of the target table, a `sqlalchemy.schema.Table`, or a
                declarative model.

            dialect:
                Either a string naming one of the CSV dialects Python defines,
                or a `csv.Dialect` object to configure the CSV reader.

            truncate (bool):
                If ``True``, truncate the table and reset all sequences before
                loading anything so that it only contains rows from the CSV. The
                default is to only append rows to the table.

            cascade (bool):
                If ``True`` and ``truncate`` is also ``True``, then the truncate
                will cascade to rows in other tables that reference it. If
                ``False`` (the default), then the truncate won't cascade, and
                will throw an exception if there are any other tables with rows
                referencing this table.

        Returns (int):
            The number of rows inserted into the table.
        """
        if isinstance(table, str):
            table_obj = self.get_table(table)
        elif hasattr(table, '__table__'):
            table_obj = table.__table__
        else:
            table_obj = table

        if truncate:
            schema_name, _sep, table_name = table_obj.fullname.partition('.')
            quote = self.id_quoter.quote

            if table_name:
                quoted_table = '%s.%s' % (quote(schema_name), quote(table_name))
            else:
                quoted_table = quote(schema_name)

            cascade_clause = 'CASCADE' if cascade else ''

            self._conn.execute('TRUNCATE TABLE ONLY %s RESTART IDENTITY %s'
                               % (quoted_table, cascade_clause))

        if isinstance(csv_source, str):
            with open(csv_source, 'r') as fdesc:
                data_rows = list(csv.DictReader(fdesc, dialect=dialect))
        else:
            data_rows = list(csv.DictReader(csv_source, dialect=dialect))

        self._conn.execute(table_obj.insert().values(data_rows))
        return len(data_rows)

    @abc.abstractmethod
    def __enter__(self):
        """Start a transaction that will be rolled back upon exit."""

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Roll back all changes made while inside the context manager."""

    @abc.abstractmethod
    def rollback(self):
        """Roll back the current transaction in a connection/engine/session
        agnostic way."""

    @classmethod
    @abc.abstractmethod
    def create_fixture(cls, name, engine_name='pg_engine',
                       use_restore_state=True, **fixture_kwargs):
        """Create a database fixture function using an instance of this class.

        Arguments:
            name (str):
                The name of the database fixture to create. This must be unique,
                so you can't use the names of any fixtures defined by this
                plugin.

            engine_name (str):
                The name of the engine fixture to use. The engine is lazily
                retrieved, so it only needs to be accessible at runtime.
                Default: ``pg_engine``

            use_restore_state (bool):
                Whether to use a restore state. See the documentation for the
                ``restore_state`` constructor parameter for more details.
                Default: ``True``

            **fixture_kwargs:
                Keyword arguments to pass to the ``pytest.fixture`` decorator.

        Returns (`callable`):
            A pytest fixture that returns an instance of a `PostgreSQLTestDBBase`
            subclass.
        """


class PostgreSQLTestDB(PostgreSQLTestDBBase):
    """A PostgreSQL test database that performs a full reset when a test finishes.

    Unless your test cannot run in one transaction, it's advised that you prefer
    `TransactedPostgreSQLTestDB` instead, since teardown is faster.

    Arguments:
        url (str):
            The connection URI of the PostgreSQL test database.

        engine (`sqlalchemy.engine.Engine`):
            The engine to use for database operations.

        restore_state (dict):
            Optional. A snapshot of the state to restore the database to after
            each test. If not given, the only cleanup that can be performed is a
            rollback of the current transaction.

            .. seealso:: `create_database_snapshot`
    """
    def __init__(self, url, engine, restore_state=None):
        super().__init__(url, engine, restore_state)
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.rollback()
        if self.is_dirty():
            self.reset_db()

    def rollback(self):
        return self.session.rollback()

    @classmethod
    def create_fixture(cls, name, engine_name='pg_engine',
                       use_restore_state=True, **fixture_kwargs):
        """See :meth:`PostgreSQLTestDBBase.create_fixture`."""
        @pytest.fixture(name=name, **fixture_kwargs)
        def _fixture(database_uri, request):
            engine = request.getfixturevalue(engine_name)

            if use_restore_state:
                restore_state = request.getfixturevalue('database_snapshot')
            else:   # pragma: no cover
                restore_state = None

            with cls(database_uri, engine, restore_state) as inst:
                yield inst

        return _fixture


class TransactedPostgreSQLTestDB(PostgreSQLTestDBBase):
    """A PostgreSQL test database that rolls back the current transaction when a
    test finishes.

    Arguments:
        url (str):
            The connection URI of the PostgreSQL test database.

        connection (`sqlalchemy.engine.Connection`):
            The connection to use for database operations.

        restore_state (dict):
            Optional. A snapshot of the state to restore the database to after
            each test. If not given, the only cleanup that can be performed is a
            rollback of the current transaction. A rollback is usually enough to
            completely reset the database and this is only needed in the event
            of an accidental ``COMMIT`` being executed.

            Database integrity will *not* be checked after the rollback if a
            restore state isn't given.

            .. seealso:: `create_database_snapshot`
    """
    def __init__(self, url, connection, restore_state=None):
        super().__init__(url, connection, restore_state)
        self.connection = connection
        self._transaction = self.connection.begin()

    def reset_db(self):
        """Reset the database by rolling back the current transaction.

        If ``restore_state`` was passed to the constructor, an exception
        will be thrown if `is_dirty` returns ``True``. Database integrity is
        *not* verified if no restore state is given to the class.

        :raises `DatabaseIsDirtyError`: `is_dirty` returned ``True``
        """
        self.time.unfreeze()
        self.rollback()

        if not self._restore_state or not self.is_dirty():
            return

        new_snapshot = create_database_snapshot(self._conn)
        raise errors.DatabaseIsDirtyError.from_snapshots(self._restore_state,
                                                         new_snapshot)

    def __enter__(self):
        # Should already be inside a transaction so there's nothing to do here.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.reset_db()

    def rollback(self):
        """Roll back the current transaction and start a new one."""
        self.session.rollback()
        self._transaction.rollback()
        self._transaction = self.connection.begin()

    @classmethod
    def create_fixture(cls, name, engine_name='pg_engine',
                       use_restore_state=True, **fixture_kwargs):
        """See :meth:`PostgreSQLTestDBBase.create_fixture`."""
        @pytest.fixture(name=name, **fixture_kwargs)
        def _fixture(database_uri, request):
            engine = request.getfixturevalue(engine_name)

            if use_restore_state:
                restore_state = request.getfixturevalue('database_snapshot')
            else:   # pragma: no cover
                restore_state = None

            with engine.connect() as conn:
                with cls(database_uri, conn, restore_state) as inst:
                    yield inst

        return _fixture
