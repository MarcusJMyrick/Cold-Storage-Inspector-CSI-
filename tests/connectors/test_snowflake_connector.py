"""Tests for SnowflakeConnector."""

import pytest
from unittest.mock import Mock, MagicMock, AsyncMock, patch
from datetime import datetime, timedelta

from csi.connectors.snowflake_connector import SnowflakeConnector
from csi.connectors.base import ConnectionConfig, ExtractionConfig
from csi.models.query import QueryRecord


@pytest.fixture
def snowflake_config():
    """Create Snowflake connection config for testing."""
    return ConnectionConfig(
        warehouse_type="SNOWFLAKE",
        account="test_account",
        user="test_user",
        password="test_password",
        warehouse="test_warehouse",
        database="test_database",
        schema="test_schema",
        role="test_role",
    )


@pytest.fixture
def mock_snowflake_connection():
    """Create mock Snowflake connection."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


@pytest.mark.asyncio
async def test_snowflake_connector_init(snowflake_config):
    """Test SnowflakeConnector initialization."""
    connector = SnowflakeConnector(snowflake_config)

    assert connector.config == snowflake_config
    assert connector._connection is None
    assert connector._cursor is None


@pytest.mark.asyncio
async def test_snowflake_connector_connect_success(snowflake_config, mock_snowflake_connection):
    """Test successful Snowflake connection."""
    conn, cursor = mock_snowflake_connection

    with patch("csi.connectors.snowflake_connector.snowflake") as mock_sf:
        mock_sf.connector.connect.return_value = conn

        connector = SnowflakeConnector(snowflake_config)
        await connector.connect()

        assert connector._connection == conn
        assert connector._cursor == cursor


@pytest.mark.asyncio
async def test_snowflake_connector_connect_missing_dependency(snowflake_config):
    """Test Snowflake connector with missing dependency."""
    with patch("csi.connectors.snowflake_connector.snowflake", None):
        connector = SnowflakeConnector(snowflake_config)

        with pytest.raises(ImportError, match="snowflake-connector-python"):
            await connector.connect()


@pytest.mark.asyncio
async def test_snowflake_connector_disconnect(snowflake_config, mock_snowflake_connection):
    """Test Snowflake disconnection."""
    conn, cursor = mock_snowflake_connection

    with patch("csi.connectors.snowflake_connector.snowflake") as mock_sf:
        mock_sf.connector.connect.return_value = conn

        connector = SnowflakeConnector(snowflake_config)
        await connector.connect()
        await connector.disconnect()

        cursor.close.assert_called_once()
        conn.close.assert_called_once()
        assert connector._connection is None
        assert connector._cursor is None


@pytest.mark.asyncio
async def test_snowflake_connector_test_connection_success(snowflake_config, mock_snowflake_connection):
    """Test connection test with successful result."""
    conn, cursor = mock_snowflake_connection
    cursor.fetchone.return_value = (1,)

    with patch("csi.connectors.snowflake_connector.snowflake") as mock_sf:
        mock_sf.connector.connect.return_value = conn

        connector = SnowflakeConnector(snowflake_config)
        await connector.connect()

        result = await connector.test_connection()
        assert result is True
        cursor.execute.assert_called_with("SELECT 1")


@pytest.mark.asyncio
async def test_snowflake_connector_test_connection_failure(snowflake_config, mock_snowflake_connection):
    """Test connection test with failure."""
    conn, cursor = mock_snowflake_connection
    cursor.execute.side_effect = Exception("Connection failed")

    with patch("csi.connectors.snowflake_connector.snowflake") as mock_sf:
        mock_sf.connector.connect.return_value = conn

        connector = SnowflakeConnector(snowflake_config)
        await connector.connect()

        result = await connector.test_connection()
        assert result is False


def test_snowflake_connector_row_to_query_record():
    """Test converting Snowflake row to QueryRecord."""
    connector = SnowflakeConnector(Mock())

    row = (
        "query_id_123",
        "SELECT * FROM test WHERE id = 123",
        "test_db",
        "test_schema",
        datetime(2024, 1, 1, 10, 0, 0),
        datetime(2024, 1, 1, 10, 0, 5),
        5000,  # elapsed_time (ms)
        1024 * 1024 * 100,  # bytes_scanned
        1000,  # rows_produced
        "test_warehouse",
        "SUCCESS",
        None,  # error_message
    )

    record = connector._row_to_query_record(row)

    assert record is not None
    assert isinstance(record, QueryRecord)
    assert record.warehouse_type == "SNOWFLAKE"
    assert record.warehouse_query_id == "query_id_123"
    assert record.query_hash is not None
    assert record.query_text == "SELECT * FROM test WHERE id = 123"
    assert record.database_name == "test_db"
    assert record.schema_name == "test_schema"
    assert record.status == "SUCCESS"
    assert record.execution_time_ms == 5000


def test_snowflake_connector_row_to_query_record_invalid_row():
    """Test converting invalid Snowflake row."""
    connector = SnowflakeConnector(Mock())

    # Invalid row (too few columns)
    row = ("query_id", "SELECT * FROM test")

    record = connector._row_to_query_record(row)

    assert record is None


def test_snowflake_connector_build_query():
    """Test building SQL query with filters."""
    connector = SnowflakeConnector(Mock())

    config = ExtractionConfig(
        start_time=datetime(2024, 1, 1, 0, 0, 0),
        end_time=datetime(2024, 1, 31, 23, 59, 59),
        database_filter="test_db",
        schema_filter="test_schema",
    )

    query = connector._build_query(config)

    assert "INFORMATION_SCHEMA.QUERY_HISTORY" in query
    assert "test_db" in query
    assert "test_schema" in query
    assert "EXECUTION_STATUS" in query


@pytest.mark.asyncio
async def test_snowflake_connector_get_table_metadata(snowflake_config, mock_snowflake_connection):
    """Test getting table metadata."""
    conn, cursor = mock_snowflake_connection
    cursor.fetchone.return_value = (
        "TABLE",
        1000,  # row_count
        1024 * 1024 * 100,  # bytes
        "date",  # clustering_key
    )

    with patch("csi.connectors.snowflake_connector.snowflake") as mock_sf:
        mock_sf.connector.connect.return_value = conn

        connector = SnowflakeConnector(snowflake_config)
        await connector.connect()

        metadata = await connector.get_table_metadata("db", "schema", "table")

        assert metadata is not None
        assert metadata.database == "db"
        assert metadata.schema == "schema"
        assert metadata.table == "table"
        assert metadata.row_count == 1000
        assert metadata.size_bytes == 1024 * 1024 * 100


@pytest.mark.asyncio
async def test_snowflake_connector_get_table_metadata_not_found(snowflake_config, mock_snowflake_connection):
    """Test getting metadata for non-existent table."""
    conn, cursor = mock_snowflake_connection
    cursor.fetchone.return_value = None

    with patch("csi.connectors.snowflake_connector.snowflake") as mock_sf:
        mock_sf.connector.connect.return_value = conn

        connector = SnowflakeConnector(snowflake_config)
        await connector.connect()

        metadata = await connector.get_table_metadata("db", "schema", "table")

        assert metadata is None


@pytest.mark.asyncio
async def test_snowflake_connector_context_manager(snowflake_config, mock_snowflake_connection):
    """Test using connector as async context manager."""
    conn, cursor = mock_snowflake_connection

    with patch("csi.connectors.snowflake_connector.snowflake") as mock_sf:
        mock_sf.connector.connect.return_value = conn

        async with SnowflakeConnector(snowflake_config) as connector:
            assert connector._connection == conn
            assert connector._cursor == cursor

        # Should be disconnected after context exit
        assert connector._connection is None
        assert connector._cursor is None

