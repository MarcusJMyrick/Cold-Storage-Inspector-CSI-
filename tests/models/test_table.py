"""Tests for TableMetadata model."""

import pytest
from csi.models.table import TableMetadata, PartitionDefinition


def test_table_metadata_creation():
    """Test creating a TableMetadata."""
    metadata = TableMetadata(
        database="analytics",
        schema="public",
        table="sales",
        warehouse_type="SNOWFLAKE",
    )
    
    assert metadata.database == "analytics"
    assert metadata.schema == "public"
    assert metadata.table == "sales"
    assert metadata.full_name == "analytics.public.sales"
    assert metadata.table_id == "analytics.public.sales"


def test_table_metadata_with_partition_key():
    """Test TableMetadata with partition definition."""
    partition_def = PartitionDefinition(
        column="date",
        type="date",
        granularity="day"
    )
    
    metadata = TableMetadata(
        database="analytics",
        schema="public",
        table="sales",
        partition_key=partition_def,
        warehouse_type="SNOWFLAKE",
    )
    
    assert metadata.partition_key is not None
    assert metadata.partition_key.column == "date"
    assert metadata.partition_key.type == "date"


def test_table_metadata_string_representation():
    """Test TableMetadata string representation."""
    metadata = TableMetadata(
        database="db",
        schema="schema",
        table="table",
        warehouse_type="SNOWFLAKE",
    )
    
    assert str(metadata) == "db.schema.table"

