"""Factory for creating warehouse connectors."""

from typing import Type, Dict, Optional
from csi.connectors.base import BaseWarehouseConnector, ConnectionConfig
from csi.models.enums import WarehouseType


# Registry of connector classes
CONNECTOR_REGISTRY: Dict[str, Type[BaseWarehouseConnector]] = {}


def register_connector(warehouse_type: str, connector_class: Type[BaseWarehouseConnector]) -> None:
    """
    Register a connector class for a warehouse type.

    Args:
        warehouse_type: Warehouse type identifier (e.g., "SNOWFLAKE")
        connector_class: Connector class that implements BaseWarehouseConnector

    Example:
        >>> register_connector("SNOWFLAKE", SnowflakeConnector)
    """
    CONNECTOR_REGISTRY[warehouse_type.upper()] = connector_class


def create_connector(config: ConnectionConfig) -> BaseWarehouseConnector:
    """
    Create a connector instance for the specified warehouse type.

    Args:
        config: Connection configuration

    Returns:
        BaseWarehouseConnector instance

    Raises:
        ValueError: If warehouse type is not supported

    Example:
        >>> config = ConnectionConfig(warehouse_type="SNOWFLAKE", ...)
        >>> connector = create_connector(config)
        >>> assert isinstance(connector, SnowflakeConnector)
    """
    warehouse_type = config.warehouse_type.upper()

    if warehouse_type not in CONNECTOR_REGISTRY:
        supported = ", ".join(CONNECTOR_REGISTRY.keys())
        raise ValueError(
            f"Unsupported warehouse type: {warehouse_type}. "
            f"Supported types: {supported}"
        )

    connector_class = CONNECTOR_REGISTRY[warehouse_type]
    return connector_class(config)


def get_supported_warehouses() -> list[str]:
    """
    Get list of supported warehouse types.

    Returns:
        List of supported warehouse type identifiers

    Example:
        >>> warehouses = get_supported_warehouses()
        >>> "SNOWFLAKE" in warehouses
        True
    """
    return list(CONNECTOR_REGISTRY.keys())


# Auto-register connectors when imported (lazy loading to avoid circular imports)
def _lazy_register_connectors():
    """Lazy register connectors to avoid circular imports."""
    # This will be called when connectors are imported
    # Actual registration happens in each connector's __init__.py
    pass

