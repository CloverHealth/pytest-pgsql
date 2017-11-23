"""Basic tests for the fixtures."""


def test_extensions_option(pg_engine):
    """Verifies the --pg-extensions option works.

    This test entirely relies on the test suite being executed with::

        --pg-extensions=btree_gin,,btree_gist
    """
    are_installed = pg_engine.execute("""
        SELECT
          EXISTS(SELECT 1 FROM pg_extension WHERE extname='btree_gin' LIMIT 1)
          AND EXISTS(SELECT 1 FROM pg_extension WHERE extname='btree_gist' LIMIT 1)
    """).scalar()

    assert are_installed, \
        "'btree_gin' and 'btree_gist' should've been installed automatically."
