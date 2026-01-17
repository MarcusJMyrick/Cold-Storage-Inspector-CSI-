"""Partition heatmap models."""

from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional
from pydantic import BaseModel, Field


class PartitionValue(str):
    """Type alias for partition value."""

    pass


class AccessMatrix(BaseModel):
    """Time-series access matrix for partition accesses."""

    # Dictionary mapping date (YYYY-MM-DD) to query count
    access_counts: Dict[str, int] = Field(default_factory=dict)

    def add_access(self, date: datetime) -> None:
        """Record an access for a specific date."""
        date_key = date.strftime("%Y-%m-%d")
        self.access_counts[date_key] = self.access_counts.get(date_key, 0) + 1

    def get_access_count(self, date: datetime) -> int:
        """Get access count for a specific date."""
        date_key = date.strftime("%Y-%m-%d")
        return self.access_counts.get(date_key, 0)

    def get_total_accesses(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> int:
        """Get total accesses in date range."""
        if not start_date and not end_date:
            return sum(self.access_counts.values())

        total = 0
        for date_str, count in self.access_counts.items():
            date = datetime.strptime(date_str, "%Y-%m-%d")
            if start_date and date < start_date:
                continue
            if end_date and date > end_date:
                continue
            total += count
        return total


class PartitionHeatMap(BaseModel):
    """
    Time-series heatmap of partition access frequency.

    Tracks query access patterns for individual table partitions to identify
    cold partitions eligible for archival.
    """

    table_id: str  # "database.schema.table"
    partition_key: str  # Column name (e.g., "date", "event_timestamp")
    partition_value: str  # Partition value (e.g., "2024-01-01")

    # Access patterns
    access_matrix: AccessMatrix = Field(default_factory=AccessMatrix)
    last_accessed: Optional[datetime] = None
    access_velocity: float = 0.0  # Queries per day (7-day moving average)

    # Derived metrics
    coldness_score: float = Field(default=1.0, ge=0.0, le=1.0)  # 1.0 = never accessed in 90 days
    estimated_savings_usd: float = 0.0
    dependent_queries: Set[str] = Field(default_factory=set)  # Set of query_hashes

    # Metadata
    data_size_bytes: Optional[int] = None
    row_count: Optional[int] = None

    @property
    def days_since_access(self) -> Optional[int]:
        """Calculate days since last access."""
        if self.last_accessed is None:
            return None
        return (datetime.utcnow() - self.last_accessed).days

    def update_coldness_score(self, lookback_days: int = 90) -> None:
        """Compute coldness score based on access patterns."""
        now = datetime.utcnow()
        end_30d = now
        start_30d = datetime(now.year, now.month, now.day) - timedelta(days=30)
        end_90d = now
        start_90d = datetime(now.year, now.month, now.day) - timedelta(days=lookback_days)

        accesses_30d = self.access_matrix.get_total_accesses(start_30d, end_30d)
        accesses_90d = self.access_matrix.get_total_accesses(start_90d, end_90d)

        if accesses_90d == 0:
            self.coldness_score = 1.0
        else:
            self.coldness_score = max(0.0, min(1.0, 1.0 - (accesses_30d / accesses_90d)))

