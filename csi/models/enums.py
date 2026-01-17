"""Enumerations for CSI models."""

from enum import Enum


class WarehouseType(str, Enum):
    """Supported data warehouse types."""

    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    DATABRICKS = "databricks"
    REDSHIFT = "redshift"


class QueryStatus(str, Enum):
    """Query execution status."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RUNNING = "RUNNING"
    QUEUED = "QUEUED"
    CANCELLED = "CANCELLED"


class QueryType(str, Enum):
    """SQL query types."""

    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    MERGE = "MERGE"
    COPY = "COPY"
    UNKNOWN = "UNKNOWN"


class EnforcementAction(str, Enum):
    """Archival policy enforcement actions."""

    DRY_RUN = "DRY_RUN"
    SCHEDULED = "SCHEDULED"
    IMMEDIATE = "IMMEDIATE"


