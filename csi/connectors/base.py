"""Base warehouse connector interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterator, Optional, Dict, Any

from csi.models.query import QueryRecord
from csi.models.table import TableMetadata


@dataclass
class ConnectionConfig:
    """Configuration for warehouse connection."""

    warehouse_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    account: Optional[str] = None  # Snowflake-specific
    warehouse: Optional[str] = None  # Snowflake-specific
    role: Optional[str] = None  # Snowflake-specific
    project_id: Optional[str] = None  # BigQuery-specific
    credentials_path: Optional[str] = None  # BigQuery-specific
    credentials_json: Optional[str] = None  # BigQuery-specific
    # Additional warehouse-specific config
    extra: Dict[str, Any] = None

    def __post_init__(self):
        """Initialize default values."""
        if self.extra is None:
            self.extra = {}


@dataclass
class ExtractionConfig:
    """Configuration for query log extraction."""

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    lookback_days: int = 90
    database_filter: Optional[str] = None
    schema_filter: Optional[str] = None
    table_filter: Optional[str] = None
    status_filter: Optional[str] = None  # Only extract SUCCESS queries, etc.
    limit: Optional[int] = None  # Maximum number of queries to extract
    batch_size: int = 1000  # Batch size for pagination

    def __post_init__(self):
        """Initialize default values."""
        if self.end_time is None:
            self.end_time = datetime.utcnow()
        if self.start_time is None:
            from datetime import timedelta
            self.start_time = self.end_time - timedelta(days=self.lookback_days)


class BaseWarehouseConnector(ABC):
    """
    Abstract base class for warehouse connectors.

    Each warehouse type (Snowflake, BigQuery, Databricks) implements this interface
    to provide a unified way to extract query logs and metadata.
    """

    def __init__(self, config: ConnectionConfig):
        """
        Initialize connector with connection configuration.

        Args:
            config: Connection configuration for the warehouse
        """
        self.config = config
        self._connection = None

    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection to the warehouse.

        Should be called before any other operations.
        Raises ConnectionError if connection fails.
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the warehouse."""
        pass

    @abstractmethod
    async def test_connection(self) -> bool:
        """
        Test if connection is working.

        Returns:
            True if connection is working, False otherwise
        """
        pass

    @abstractmethod
    async def extract_query_logs(
        self,
        config: Optional[ExtractionConfig] = None,
    ) -> AsyncIterator[QueryRecord]:
        """
        Extract query logs from the warehouse.

        Yields QueryRecord instances for each query execution found.

        Args:
            config: Extraction configuration (optional, uses defaults if not provided)

        Yields:
            QueryRecord instances

        Example:
            >>> connector = SnowflakeConnector(config)
            >>> async for record in connector.extract_query_logs():
            ...     print(record.query_hash)
        """
        pass

    @abstractmethod
    async def get_table_metadata(
        self,
        database: str,
        schema: str,
        table: str,
    ) -> Optional[TableMetadata]:
        """
        Get metadata for a specific table.

        Args:
            database: Database name
            schema: Schema name
            table: Table name

        Returns:
            TableMetadata instance, or None if table not found
        """
        pass

    @abstractmethod
    async def get_storage_info(self) -> Dict[str, Any]:
        """
        Get storage information for the warehouse.

        Returns:
            Dictionary with storage metrics (size, cost, etc.)
        """
        pass

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

