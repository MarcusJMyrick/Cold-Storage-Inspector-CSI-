"""Query execution record models."""

import uuid
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


class TableRef(BaseModel):
    """Reference to a table in a query."""

    database: str
    schema: str
    table: str

    @property
    def full_name(self) -> str:
        """Return fully qualified table name."""
        return f"{self.database}.{self.schema}.{self.table}"

    def __str__(self) -> str:
        return self.full_name


class PartitionRef(BaseModel):
    """Reference to a partition in a query predicate."""

    column: str
    operator: str  # '=', '>', '>=', '<', '<=', '!=', 'BETWEEN', 'IN'
    value: Optional[str] = None
    value_low: Optional[str] = None  # For BETWEEN
    value_high: Optional[str] = None  # For BETWEEN
    values: Optional[List[str]] = None  # For IN

    def __str__(self) -> str:
        if self.operator == "BETWEEN":
            return f"{self.column} BETWEEN {self.value_low} AND {self.value_high}"
        elif self.operator == "IN":
            values_str = ", ".join(self.values or [])
            return f"{self.column} IN ({values_str})"
        else:
            return f"{self.column} {self.operator} {self.value}"


class QueryRecord(BaseModel):
    """
    Canonical representation of a single query execution.

    This model represents the unified semantic model for query execution metadata
    extracted from heterogeneous warehouse sources.
    """

    # Identity
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    warehouse_type: str  # WarehouseType enum value
    warehouse_query_id: str  # Native warehouse ID (e.g., Snowflake QUERY_ID)
    query_hash: str = Field(..., max_length=16)  # Deterministic hash of normalized query
    query_text: str = Field(..., max_length=1_000_000)  # Raw SQL (1MB limit)
    query_text_normalized: str  # SQL with literals masked
    query_type: str  # QueryType enum value

    # Execution Context
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_refs: List[TableRef] = Field(default_factory=list)
    partition_refs: List[PartitionRef] = Field(default_factory=list)

    # Temporal Metadata
    start_time: datetime
    end_time: Optional[datetime] = None
    execution_time_ms: Optional[int] = None

    # Resource Consumption
    bytes_scanned: Optional[int] = None
    bytes_written: Optional[int] = None
    rows_produced: Optional[int] = None
    partitions_scanned: Optional[int] = None
    partitions_total: Optional[int] = None

    # Cost Attribution
    estimated_cost_usd: Optional[float] = None
    credits_used: Optional[float] = None  # Warehouse-specific
    slot_ms: Optional[int] = None  # BigQuery-specific

    # Status & Errors
    status: str  # QueryStatus enum value
    error_message: Optional[str] = None

    # CSI Metadata
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    tenant_id: Optional[str] = None  # For multi-tenancy

    class Config:
        """Pydantic configuration."""

        json_encoders = {
            datetime: lambda v: v.isoformat(),
            uuid.UUID: str,
        }


