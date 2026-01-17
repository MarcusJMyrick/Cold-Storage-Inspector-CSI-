"""Tests for PartitionHeatMap model."""

import pytest
from datetime import datetime, timedelta
from csi.models.heatmap import PartitionHeatMap, AccessMatrix


def test_access_matrix_add_access():
    """Test adding accesses to AccessMatrix."""
    matrix = AccessMatrix()
    date = datetime(2024, 1, 15)
    
    matrix.add_access(date)
    matrix.add_access(date)  # Second access on same day
    
    assert matrix.get_access_count(date) == 2
    assert matrix.get_total_accesses() == 2


def test_access_matrix_date_range():
    """Test AccessMatrix date range filtering."""
    matrix = AccessMatrix()
    
    # Add accesses across multiple days
    for i in range(10):
        date = datetime(2024, 1, i + 1)
        matrix.add_access(date)
    
    # Get total accesses
    assert matrix.get_total_accesses() == 10
    
    # Get accesses in date range
    start = datetime(2024, 1, 5)
    end = datetime(2024, 1, 8)
    assert matrix.get_total_accesses(start, end) == 4


def test_partition_heatmap_creation():
    """Test creating a PartitionHeatMap."""
    heatmap = PartitionHeatMap(
        table_id="analytics.public.sales",
        partition_key="date",
        partition_value="2024-01-01",
    )
    
    assert heatmap.table_id == "analytics.public.sales"
    assert heatmap.partition_key == "date"
    assert heatmap.partition_value == "2024-01-01"
    assert heatmap.coldness_score == 1.0  # Default (no accesses)


def test_partition_heatmap_days_since_access():
    """Test days_since_access property."""
    heatmap = PartitionHeatMap(
        table_id="test.table",
        partition_key="date",
        partition_value="2024-01-01",
    )
    
    # No access yet
    assert heatmap.days_since_access is None
    
    # Set last access
    heatmap.last_accessed = datetime.utcnow() - timedelta(days=10)
    assert heatmap.days_since_access == 10


def test_partition_heatmap_coldness_score_no_access():
    """Test coldness score calculation with no accesses."""
    heatmap = PartitionHeatMap(
        table_id="test.table",
        partition_key="date",
        partition_value="2024-01-01",
    )
    
    heatmap.update_coldness_score(lookback_days=90)
    assert heatmap.coldness_score == 1.0  # Completely cold


def test_partition_heatmap_coldness_score_with_access():
    """Test coldness score calculation with accesses."""
    heatmap = PartitionHeatMap(
        table_id="test.table",
        partition_key="date",
        partition_value="2024-01-01",
    )
    
    # Add 10 accesses in last 30 days, 20 in last 90 days
    now = datetime.utcnow()
    for i in range(20):
        date = now - timedelta(days=i)
        heatmap.access_matrix.add_access(date)
    
    heatmap.update_coldness_score(lookback_days=90)
    
    # Should have accesses, so coldness < 1.0
    assert 0.0 < heatmap.coldness_score < 1.0


def test_partition_heatmap_coldness_score_recent_access():
    """Test coldness score when partition is recently accessed."""
    heatmap = PartitionHeatMap(
        table_id="test.table",
        partition_key="date",
        partition_value="2024-01-01",
    )
    
    # Add 30 accesses in last 30 days, 30 in last 90 days
    now = datetime.utcnow()
    for i in range(30):
        date = now - timedelta(days=i)
        heatmap.access_matrix.add_access(date)
    
    heatmap.update_coldness_score(lookback_days=90)
    
    # All accesses in last 30 days = warm partition
    # coldness_score should be low
    assert heatmap.coldness_score < 0.5


def test_partition_heatmap_dependent_queries():
    """Test tracking dependent queries."""
    heatmap = PartitionHeatMap(
        table_id="test.table",
        partition_key="date",
        partition_value="2024-01-01",
    )
    
    heatmap.dependent_queries.add("query_hash_1")
    heatmap.dependent_queries.add("query_hash_2")
    
    assert len(heatmap.dependent_queries) == 2
    assert "query_hash_1" in heatmap.dependent_queries

