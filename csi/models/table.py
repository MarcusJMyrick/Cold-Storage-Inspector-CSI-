"""Table metadata models."""

from typing import Optional, List, Dict
from pydantic import BaseModel


class PartitionDefinition(BaseModel):
    """Partition key definition for a table."""

    column: str
    type: str  # 'date', 'timestamp', 'integer', etc.
    granularity: Optional[str] = None  # 'day', 'month', 'year'


class TableMetadata(BaseModel):
    """Metadata about a data warehouse table."""

    database: str
    schema: str
    table: str
    partition_key: Optional[PartitionDefinition] = None
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    warehouse_type: str
    additional_metadata: Dict[str, str] = {}

    @property
    def full_name(self) -> str:
        """Return fully qualified table name."""
        return f"{self.database}.{self.schema}.{self.table}"

    @property
    def table_id(self) -> str:
        """Return table identifier for CSI."""
        return self.full_name

    def __str__(self) -> str:
        return self.full_name


