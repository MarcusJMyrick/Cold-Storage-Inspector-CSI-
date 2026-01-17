"""Tests for BaseWarehouseConnector."""

import pytest
from datetime import datetime, timedelta
from csi.connectors.base import BaseWarehouseConnector, ConnectionConfig, ExtractionConfig


def test_connection_config_creation():
    """Test creating a ConnectionConfig."""
    config = ConnectionConfig(
        warehouse_type="SNOWFLAKE",
        host="example.snowflakecomputing.com",
        database="analytics",
        user="user",
        password="password",
    )

    assert config.warehouse_type == "SNOWFLAKE"
    assert config.host == "example.snowflakecomputing.com"
    assert config.database == "analytics"


def test_extraction_config_defaults():
    """Test ExtractionConfig default values."""
    config = ExtractionConfig()

    assert config.lookback_days == 90
    assert config.batch_size == 1000
    assert config.start_time is not None
    assert config.end_time is not None
    assert config.end_time > config.start_time


def test_extraction_config_custom_values():
    """Test ExtractionConfig with custom values."""
    start = datetime.utcnow() - timedelta(days=30)
    end = datetime.utcnow()

    config = ExtractionConfig(
        start_time=start,
        end_time=end,
        lookback_days=30,
        limit=1000,
    )

    assert config.start_time == start
    assert config.end_time == end
    assert config.limit == 1000

