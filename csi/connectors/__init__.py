"""Warehouse connector modules for CSI."""

from csi.connectors.base import BaseWarehouseConnector, ConnectionConfig, ExtractionConfig
from csi.connectors.factory import create_connector, _lazy_register_connectors

# Auto-register available connectors
_lazy_register_connectors()

__all__ = [
    "BaseWarehouseConnector",
    "ConnectionConfig",
    "ExtractionConfig",
    "create_connector",
]

