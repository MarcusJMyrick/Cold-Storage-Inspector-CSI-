"""Shared pytest fixtures and configuration."""

import pytest
from datetime import datetime
from typing import List
import uuid

from csi.models.query import QueryRecord, TableRef, PartitionRef
from csi.models.table import TableMetadata
from csi.models.heatmap import PartitionHeatMap, AccessMatrix
from csi.models.policy import ArchivalPolicy


@pytest.fixture
def sample_query_record() -> QueryRecord:
    """Create a sample QueryRecord for testing."""
    return QueryRecord(
        warehouse_type="SNOWFLAKE",
        warehouse_query_id="test_query_123",
        query_hash="abc123def456",
        query_text="SELECT * FROM analytics.public.sales WHERE date = '2024-01-01'",
        query_text_normalized="SELECT * FROM analytics.public.sales WHERE date = ?",
        query_type="SELECT",
        database_name="analytics",
        schema_name="public",
        table_refs=[
            TableRef(database="analytics", schema="public", table="sales")
        ],
        partition_refs=[
            PartitionRef(column="date", operator="=", value="2024-01-01")
        ],
        start_time=datetime(2024, 1, 15, 10, 0, 0),
        end_time=datetime(2024, 1, 15, 10, 0, 5),
        execution_time_ms=5000,
        bytes_scanned=1024 * 1024 * 100,  # 100 MB
        status="SUCCESS",
    )


@pytest.fixture
def sample_query_records() -> List[QueryRecord]:
    """Create multiple sample QueryRecords for testing."""
    records = []
    base_date = datetime(2024, 1, 1, 10, 0, 0)
    
    for i in range(10):
        record = QueryRecord(
            warehouse_type="SNOWFLAKE",
            warehouse_query_id=f"query_{i}",
            query_hash=f"hash_{i % 3}",  # Some duplicate queries
            query_text=f"SELECT * FROM sales WHERE date = '2024-01-{i+1:02d}'",
            query_text_normalized="SELECT * FROM sales WHERE date = ?",
            query_type="SELECT",
            start_time=base_date.replace(day=i+1),
            bytes_scanned=1024 * 1024 * 50,
            status="SUCCESS",
        )
        records.append(record)
    
    return records


@pytest.fixture
def sample_table_metadata() -> TableMetadata:
    """Create a sample TableMetadata for testing."""
    from csi.models.table import PartitionDefinition
    
    return TableMetadata(
        database="analytics",
        schema="public",
        table="sales",
        partition_key=PartitionDefinition(
            column="date",
            type="date",
            granularity="day"
        ),
        size_bytes=1024 * 1024 * 1024 * 100,  # 100 GB
        row_count=1_000_000_000,
        warehouse_type="SNOWFLAKE",
    )


@pytest.fixture
def sample_partition_heatmap() -> PartitionHeatMap:
    """Create a sample PartitionHeatMap for testing."""
    heatmap = PartitionHeatMap(
        table_id="analytics.public.sales",
        partition_key="date",
        partition_value="2024-01-01",
    )
    # Add some access history
    for i in range(30):
        date = datetime(2024, 1, i + 1)
        heatmap.access_matrix.add_access(date)
    heatmap.last_accessed = datetime(2024, 1, 30)
    heatmap.update_coldness_score()
    return heatmap

