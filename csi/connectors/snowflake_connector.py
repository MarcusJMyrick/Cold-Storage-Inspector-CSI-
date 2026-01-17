"""Snowflake warehouse connector implementation."""

import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, AsyncIterator, List
import logging

from csi.connectors.base import BaseWarehouseConnector, ConnectionConfig, ExtractionConfig
from csi.models.query import QueryRecord, TableRef, PartitionRef
from csi.models.table import TableMetadata
from csi.utils.query_normalization import normalize_query, compute_query_hash

logger = logging.getLogger(__name__)

# Snowflake query status mapping
STATUS_MAP = {
    "SUCCESS": "SUCCESS",
    "FAILED": "FAILED",
    "RUNNING": "RUNNING",
    "QUEUED": "QUEUED",
}

# Template SQL query for extracting query history
QUERY_HISTORY_SQL = """
SELECT
    QUERY_ID,
    QUERY_TEXT,
    DATABASE_NAME,
    SCHEMA_NAME,
    START_TIME,
    END_TIME,
    TOTAL_ELAPSED_TIME,
    BYTES_SCANNED,
    ROWS_PRODUCED,
    WAREHOUSE_NAME,
    EXECUTION_STATUS,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    RESULT_LIMIT => {limit},
    END_TIME_RANGE_START => '{start_time}'::timestamp_ntz,
    END_TIME_RANGE_END => '{end_time}'::timestamp_ntz
))
WHERE EXECUTION_STATUS IN ('SUCCESS', 'FAILED')
ORDER BY START_TIME DESC
"""


class SnowflakeConnector(BaseWarehouseConnector):
    """Snowflake warehouse connector."""

    def __init__(self, config: ConnectionConfig):
        """
        Initialize Snowflake connector.

        Args:
            config: Connection configuration for Snowflake
        """
        super().__init__(config)
        self._connection = None
        self._cursor = None

    async def connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            # Lazy import to avoid requiring snowflake-connector-python for all installs
            import snowflake.connector

            logger.info(f"Connecting to Snowflake account: {self.config.account}")

            # Build connection parameters
            conn_params = {
                "account": self.config.account,
                "user": self.config.user,
                "password": self.config.password,
                "warehouse": self.config.warehouse,
                "database": self.config.database,
                "schema": self.config.schema,
                "role": self.config.role,
            }

            # Remove None values
            conn_params = {k: v for k, v in conn_params.items() if v is not None}

            # Add any extra parameters
            if self.config.extra:
                conn_params.update(self.config.extra)

            # Create connection (sync, but run in executor for async compatibility)
            loop = asyncio.get_event_loop()
            self._connection = await loop.run_in_executor(
                None,
                lambda: snowflake.connector.connect(**conn_params)
            )

            self._cursor = self._connection.cursor()

            logger.info("Successfully connected to Snowflake")

        except ImportError:
            raise ImportError(
                "snowflake-connector-python is required for Snowflake connector. "
                "Install with: pip install cold-storage-inspector[snowflake]"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise ConnectionError(f"Failed to connect to Snowflake: {e}") from e

    async def disconnect(self) -> None:
        """Close connection to Snowflake."""
        try:
            if self._cursor:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._cursor.close)
                self._cursor = None

            if self._connection:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._connection.close)
                self._connection = None

            logger.info("Disconnected from Snowflake")

        except Exception as e:
            logger.warning(f"Error disconnecting from Snowflake: {e}")

    async def test_connection(self) -> bool:
        """Test if connection is working."""
        try:
            if not self._connection or not self._cursor:
                return False

            # Execute a simple query
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._cursor.execute("SELECT 1")
            )

            result = await loop.run_in_executor(
                None,
                lambda: self._cursor.fetchone()
            )

            return result is not None and result[0] == 1

        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    async def extract_query_logs(
        self,
        config: Optional[ExtractionConfig] = None,
    ) -> AsyncIterator[QueryRecord]:
        """
        Extract query logs from Snowflake.

        Yields QueryRecord instances for each query execution found.

        Args:
            config: Extraction configuration (optional)

        Yields:
            QueryRecord instances
        """
        if not self._connection or not self._cursor:
            raise ConnectionError("Not connected to Snowflake. Call connect() first.")

        if config is None:
            config = ExtractionConfig()

        logger.info(f"Extracting query logs from {config.start_time} to {config.end_time}")

        # Build query with filters
        query = self._build_query(config)

        # Execute query with pagination
        offset = 0
        limit = config.batch_size
        total_yielded = 0

        while True:
            # Add pagination
            paginated_query = f"{query} LIMIT {limit} OFFSET {offset}"

            try:
                # Execute query (sync, run in executor)
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: self._cursor.execute(paginated_query)
                )

                # Fetch results
                rows = await loop.run_in_executor(
                    None,
                    lambda: self._cursor.fetchmany(limit)
                )

                if not rows:
                    break

                # Convert rows to QueryRecord
                for row in rows:
                    query_record = self._row_to_query_record(row)
                    if query_record:
                        yield query_record
                        total_yielded += 1

                        # Check limit
                        if config.limit and total_yielded >= config.limit:
                            return

                # Check if we got fewer rows than requested (end of results)
                if len(rows) < limit:
                    break

                offset += limit

            except Exception as e:
                logger.error(f"Error extracting query logs: {e}")
                raise

        logger.info(f"Extracted {total_yielded} query records")

    def _build_query(self, config: ExtractionConfig) -> str:
        """Build SQL query with filters."""
        # Base query template
        query = QUERY_HISTORY_SQL.format(
            limit=config.limit or 10000,
            start_time=config.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            end_time=config.end_time.strftime("%Y-%m-%d %H:%M:%S"),
        )

        # Add database filter if specified
        if config.database_filter:
            query = query.replace(
                "WHERE EXECUTION_STATUS",
                f"WHERE DATABASE_NAME = '{config.database_filter}' AND EXECUTION_STATUS"
            )

        # Add schema filter if specified
        if config.schema_filter:
            query = query.replace(
                "WHERE EXECUTION_STATUS",
                f"WHERE SCHEMA_NAME = '{config.schema_filter}' AND EXECUTION_STATUS"
            )

        return query

    def _row_to_query_record(self, row: tuple) -> Optional[QueryRecord]:
        """
        Convert Snowflake query history row to QueryRecord.

        Args:
            row: Row from query history table

        Returns:
            QueryRecord instance, or None if conversion fails
        """
        try:
            (
                query_id,
                query_text,
                database_name,
                schema_name,
                start_time,
                end_time,
                elapsed_time,
                bytes_scanned,
                rows_produced,
                warehouse_name,
                status,
                error_message,
            ) = row

            # Normalize query text
            normalized_text = normalize_query(query_text)

            # Compute query hash
            query_hash = compute_query_hash(query_text, normalized_text)

            # Convert status
            status_str = STATUS_MAP.get(status, status) if status else "UNKNOWN"

            # Parse table references (simplified - will be enhanced with SQL parser)
            table_refs = []
            if database_name and schema_name:
                # Extract table names from query (simplified)
                # Full implementation will use SQL parser
                table_refs = [TableRef(database=database_name, schema=schema_name, table="unknown")]

            # Calculate execution time in milliseconds
            execution_time_ms = int(elapsed_time) if elapsed_time else None

            # Create QueryRecord
            record = QueryRecord(
                warehouse_type="SNOWFLAKE",
                warehouse_query_id=str(query_id),
                query_hash=query_hash,
                query_text=query_text[:1_000_000],  # 1MB limit
                query_text_normalized=normalized_text,
                query_type="SELECT",  # Will be parsed from query text
                database_name=database_name,
                schema_name=schema_name,
                table_refs=table_refs,
                start_time=start_time,
                end_time=end_time,
                execution_time_ms=execution_time_ms,
                bytes_scanned=int(bytes_scanned) if bytes_scanned else None,
                rows_produced=int(rows_produced) if rows_produced else None,
                status=status_str,
                error_message=error_message,
            )

            return record

        except Exception as e:
            logger.warning(f"Failed to convert row to QueryRecord: {e}")
            return None

    async def get_table_metadata(
        self,
        database: str,
        schema: str,
        table: str,
    ) -> Optional[TableMetadata]:
        """Get metadata for a specific table."""
        if not self._connection or not self._cursor:
            raise ConnectionError("Not connected to Snowflake. Call connect() first.")

        try:
            # Query table metadata
            query = f"""
            SELECT
                TABLE_TYPE,
                ROW_COUNT,
                BYTES,
                CLUSTERING_KEY
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = '{database}'
                AND TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table}'
            """

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._cursor.execute(query)
            )

            row = await loop.run_in_executor(
                None,
                lambda: self._cursor.fetchone()
            )

            if not row:
                return None

            table_type, row_count, bytes_value, clustering_key = row

            # Get partition key from clustering key (if available)
            partition_key = None
            if clustering_key:
                from csi.models.table import PartitionDefinition
                partition_key = PartitionDefinition(
                    column=clustering_key,
                    type="string",  # Simplified
                )

            metadata = TableMetadata(
                database=database,
                schema=schema,
                table=table,
                partition_key=partition_key,
                size_bytes=int(bytes_value) if bytes_value else None,
                row_count=int(row_count) if row_count else None,
                warehouse_type="SNOWFLAKE",
            )

            return metadata

        except Exception as e:
            logger.error(f"Failed to get table metadata: {e}")
            return None

    async def get_storage_info(self) -> Dict[str, Any]:
        """Get storage information for Snowflake."""
        if not self._connection or not self._cursor:
            raise ConnectionError("Not connected to Snowflake. Call connect() first.")

        try:
            # Query storage usage
            query = """
            SELECT
                AVG(AVERAGE_DATABASE_BYTES) as avg_storage_bytes,
                AVG(AVERAGE_FAILSAFE_BYTES) as avg_failsafe_bytes
            FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
            WHERE USAGE_DATE >= CURRENT_DATE - 30
            """

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._cursor.execute(query)
            )

            row = await loop.run_in_executor(
                None,
                lambda: self._cursor.fetchone()
            )

            avg_storage_bytes = int(row[0]) if row and row[0] else 0
            avg_failsafe_bytes = int(row[1]) if row and row[1] else 0

            return {
                "storage_bytes": avg_storage_bytes,
                "failsafe_bytes": avg_failsafe_bytes,
                "total_bytes": avg_storage_bytes + avg_failsafe_bytes,
            }

        except Exception as e:
            logger.error(f"Failed to get storage info: {e}")
            return {
                "storage_bytes": 0,
                "failsafe_bytes": 0,
                "total_bytes": 0,
            }

