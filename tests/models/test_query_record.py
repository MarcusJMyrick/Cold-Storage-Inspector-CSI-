"""Tests for QueryRecord model."""

import pytest
from datetime import datetime
from csi.models.query import QueryRecord, TableRef, PartitionRef
from csi.models.enums import QueryStatus


def test_query_record_creation():
    """Test creating a QueryRecord with required fields."""
    record = QueryRecord(
        warehouse_type="SNOWFLAKE",
        warehouse_query_id="test_123",
        query_hash="abc123",
        query_text="SELECT * FROM test",
        query_text_normalized="SELECT * FROM test",
        query_type="SELECT",
        start_time=datetime(2024, 1, 1, 10, 0, 0),
        status="SUCCESS",
    )
    
    assert record.warehouse_type == "SNOWFLAKE"
    assert record.query_hash == "abc123"
    assert record.status == "SUCCESS"
    assert record.id is not None  # UUID should be auto-generated


def test_query_record_with_table_refs():
    """Test QueryRecord with table references."""
    record = QueryRecord(
        warehouse_type="SNOWFLAKE",
        warehouse_query_id="test_123",
        query_hash="abc123",
        query_text="SELECT * FROM db.schema.table",
        query_text_normalized="SELECT * FROM db.schema.table",
        query_type="SELECT",
        start_time=datetime(2024, 1, 1, 10, 0, 0),
        status="SUCCESS",
        table_refs=[
            TableRef(database="db", schema="schema", table="table")
        ],
    )
    
    assert len(record.table_refs) == 1
    assert record.table_refs[0].full_name == "db.schema.table"


def test_query_record_with_partition_refs():
    """Test QueryRecord with partition references."""
    record = QueryRecord(
        warehouse_type="SNOWFLAKE",
        warehouse_query_id="test_123",
        query_hash="abc123",
        query_text="SELECT * FROM sales WHERE date = '2024-01-01'",
        query_text_normalized="SELECT * FROM sales WHERE date = ?",
        query_type="SELECT",
        start_time=datetime(2024, 1, 1, 10, 0, 0),
        status="SUCCESS",
        partition_refs=[
            PartitionRef(column="date", operator="=", value="2024-01-01")
        ],
    )
    
    assert len(record.partition_refs) == 1
    assert record.partition_refs[0].column == "date"
    assert str(record.partition_refs[0]) == "date = 2024-01-01"


def test_table_ref_full_name():
    """Test TableRef full_name property."""
    ref = TableRef(database="db", schema="schema", table="table")
    assert ref.full_name == "db.schema.table"
    assert str(ref) == "db.schema.table"


def test_partition_ref_str_representation():
    """Test PartitionRef string representations."""
    # Equality operator
    ref1 = PartitionRef(column="date", operator="=", value="2024-01-01")
    assert str(ref1) == "date = 2024-01-01"
    
    # BETWEEN operator
    ref2 = PartitionRef(
        column="date",
        operator="BETWEEN",
        value_low="2024-01-01",
        value_high="2024-01-31"
    )
    assert "BETWEEN" in str(ref2)
    
    # IN operator
    ref3 = PartitionRef(
        column="date",
        operator="IN",
        values=["2024-01-01", "2024-01-02"]
    )
    assert "IN" in str(ref3)


def test_query_record_json_serialization():
    """Test QueryRecord can be serialized to JSON."""
    record = QueryRecord(
        warehouse_type="SNOWFLAKE",
        warehouse_query_id="test_123",
        query_hash="abc123",
        query_text="SELECT * FROM test",
        query_text_normalized="SELECT * FROM test",
        query_type="SELECT",
        start_time=datetime(2024, 1, 1, 10, 0, 0),
        status="SUCCESS",
    )
    
    # Should not raise exception
    json_str = record.model_dump_json()
    assert isinstance(json_str, str)
    assert "test_123" in json_str

