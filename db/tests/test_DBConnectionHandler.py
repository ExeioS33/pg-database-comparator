import pytest
from unittest.mock import patch, MagicMock
from db.src.DBConnectionHandler import DbConnectionHandler


@patch('db.src.DBConnectionHandler.psycopg2.connect')
def test_get_connections_success(mock_connect):
    # Mock the connections
    mock_conn1 = MagicMock()
    mock_conn2 = MagicMock()
    mock_connect.side_effect = [mock_conn1, mock_conn2]

    db_handler = DbConnectionHandler('PG-TEST', 'PG-DWH')
    connections = db_handler.get_connections()

    # Assert that psycopg2.connect was called twice
    assert mock_connect.call_count == 2

    # Assert that connections are stored correctly
    assert connections['PG-TEST'] == mock_conn1
    assert connections['PG-DWH'] == mock_conn2


def test_get_connections_error():
    # static method call with raise error exception
    with pytest.raises(ValueError) as exc_info:
        DbConnectionHandler._connect_to_db("PG-TIERS")
    assert str(exc_info.value) == "PG-TIERS is not yet implemented"


def test_close_connections():
    # Mock connections
    mock_conn1 = MagicMock()
    mock_conn2 = MagicMock()

    db_handler = DbConnectionHandler('PG-TEST', 'PG-DWH')
    db_handler.connections = {'PG-TEST': mock_conn1, 'PG-DWH': mock_conn2}

    assert db_handler.close_connections() == True
