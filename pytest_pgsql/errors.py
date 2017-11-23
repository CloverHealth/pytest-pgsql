"""Specialized errors."""

import collections


TableInfo = collections.namedtuple('TableInfo', ['schema', 'table', 'oid'])


def _diff_snapshots(original_snapshot, current_snapshot):
    """Compare two database snapshots and return the differences.

    Arguments:
        original_snapshot (dict):
            The original snapshot of the database.

        current_snapshot (dict):
            The snapshot of the database in its current state.

    Returns (dict):
        A dictionary with information on what schemas, extensions, tables, etc.
        are left over or are missing.

    .. seealso:: :func:`create_database_snapshot`
    """
    new_schemas = set(current_snapshot['schemas'])
    old_schemas = set(original_snapshot['schemas'])
    new_ext = set(current_snapshot['extensions'])
    old_ext = set(original_snapshot['extensions'])
    new_tables = {
        TableInfo(t['schema_name'], t['table_name'], t['table_oid'])
        for t in current_snapshot['tables']
    }
    old_tables = {
        TableInfo(t['schema_name'], t['table_name'], t['table_oid'])
        for t in original_snapshot['tables']
    }

    return {
        'extra_extensions': new_ext - old_ext,
        'missing_extensions': old_ext - new_ext,
        'extra_schemas': new_schemas - old_schemas,
        'missing_schemas': old_schemas - new_schemas,
        'extra_tables': new_tables - old_tables,
        'missing_tables': old_tables - new_tables,
    }


class Error(Exception):
    """The base class for all errors.

    This exception is not meant to be thrown directly.

    Arguments:
        message (str):
            Optional. The error message for the exception to be thrown. If not
            given, defaults to the first line of the exception class' docstring.
    """
    def __init__(self, message=None):
        if not message:
            message = self.__doc__.splitlines()[0]
        super().__init__(message)


class DatabaseRestoreFailedError(Error):
    """Generic base class for database reset failures."""


class DatabaseIsDirtyError(DatabaseRestoreFailedError):
    """Couldn't restore the database to its original state due to committed
    changes.

    Arguments:
        message (str):
            The error message.

        state_details (dict):
            Optional. A dictionary detailing what extensions, schemas, or tables
            are missing or are left over. Keys include:

            - ``extra_extensions``: A `set` of the names of extensions that
              weren't originally installed but still remain.
            - ``missing_extensions``: A `set` of the names of extensions that
              were uninstalled.
            - ``extra_schemas``: A `set` of the names of schemas that weren't
              present initially but still remain.
            - ``missing_schemas``: A `set` of the names of schemas that were
              initially present but were dropped and can't be restored.
            - ``extra_tables``: A `set` of `TableInfo` objects for tables that
              weren't present at the beginning of the test session.
            - ``missing_tables``: A `set` of `TableInfo` objects for tables that
              were present at the beginning of the test session but were dropped
              and can't be restored.
    """
    def __init__(self, message=None, state_details=None):
        super().__init__(message)
        self.state_details = state_details

    @classmethod
    def from_snapshots(cls, original_snapshot, current_snapshot):
        """Create an exception with an error message derived from the given
        snapshots.

        Arguments:
            original_snapshot (dict):
                The snapshot of the database taken right after it was created.

            current_snapshot (dict):
                The snapshot of the database in its current (dirty) state.

        .. seealso:: `pytest_pgsql.database.create_database_snapshot`
        """
        state = _diff_snapshots(original_snapshot, current_snapshot)
        strings = {
            'extra_extensions': ', '.join(state['extra_extensions']) or 'None',
            'missing_extensions': ', '.join(state['missing_extensions']) or 'None',
            'extra_schemas': ', '.join(state['extra_schemas']) or 'None',
            'missing_schemas': ', '.join(state['missing_schemas']) or 'None',
            'extra_tables':
                ', '.join('{0.schema}.{0.table}'.format(t)
                          for t in state['extra_tables']) or 'None',
            'missing_tables':
                ', '.join('{0.schema}.{0.table}'.format(t)
                          for t in state['missing_tables']) or 'None',
        }

        return cls(
            "The database state wasn't reset successfully. Extra tables or "
            "schemas may remain, or preexisting tables and/or schemas may not "
            "have been restored:\n"
            " * Extra extensions: %(extra_extensions)s\n"
            " * Missing extensions: %(missing_extensions)s\n"
            " * Extra schemas: %(extra_schemas)s\n"
            " * Missing schemas: %(missing_schemas)s\n"
            " * Extra tables: %(extra_tables)s\n"
            " * Missing tables: %(missing_tables)s" % strings,
            state)


class NoSnapshotAvailableError(DatabaseRestoreFailedError):
    """Can't restore the database - no snapshot was given to the class."""
