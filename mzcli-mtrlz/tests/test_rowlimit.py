import pytest
from mock import Mock

from pgcli.main import PGCli


# We need this fixtures beacause we need PGCli object to be created
# after test collection so it has config loaded from temp directory


@pytest.fixture(scope="module")
def default_pgcli_obj():
    return PGCli()


@pytest.fixture(scope="module")
def DEFAULT(default_pgcli_obj):
    return default_pgcli_obj.row_limit


@pytest.fixture(scope="module")
def LIMIT(DEFAULT):
    return DEFAULT + 1000


@pytest.fixture(scope="module")
def over_default(DEFAULT):
    over_default_cursor = Mock()
    over_default_cursor.configure_mock(rowcount=DEFAULT + 10)
    return over_default_cursor


@pytest.fixture(scope="module")
def over_limit(LIMIT):
    over_limit_cursor = Mock()
    over_limit_cursor.configure_mock(rowcount=LIMIT + 10)
    return over_limit_cursor


@pytest.fixture(scope="module")
def low_count():
    low_count_cursor = Mock()
    low_count_cursor.configure_mock(rowcount=1)
    return low_count_cursor


def test_row_limit_with_LIMIT_clause(LIMIT, over_limit):
    cli = PGCli(row_limit=LIMIT)
    stmt = "SELECT * FROM students LIMIT 1000"

    result = cli._should_limit_output(stmt, over_limit)
    assert result is False

    cli = PGCli(row_limit=0)
    result = cli._should_limit_output(stmt, over_limit)
    assert result is False


def test_row_limit_without_LIMIT_clause(LIMIT, over_limit):
    cli = PGCli(row_limit=LIMIT)
    stmt = "SELECT * FROM students"

    result = cli._should_limit_output(stmt, over_limit)
    assert result is True

    cli = PGCli(row_limit=0)
    result = cli._should_limit_output(stmt, over_limit)
    assert result is False


def test_row_limit_on_non_select(over_limit):
    cli = PGCli()
    stmt = "UPDATE students SET name='Boby'"
    result = cli._should_limit_output(stmt, over_limit)
    assert result is False

    cli = PGCli(row_limit=0)
    result = cli._should_limit_output(stmt, over_limit)
    assert result is False
