"""
Cold Storage Inspector (CSI)

AI-powered data warehouse cost optimization through intelligent cold data archival.
"""

__version__ = "0.1.0"

from csi.models import (
    QueryRecord,
    PartitionHeatMap,
    TableMetadata,
    ArchivalPolicy,
    WarehouseType,
    QueryStatus,
)

__all__ = [
    "__version__",
    "QueryRecord",
    "PartitionHeatMap",
    "TableMetadata",
    "ArchivalPolicy",
    "WarehouseType",
    "QueryStatus",
]

