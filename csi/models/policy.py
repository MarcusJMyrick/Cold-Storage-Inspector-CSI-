"""Archival policy models."""

import uuid
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field

from csi.models.enums import EnforcementAction


class QueryRef(BaseModel):
    """Reference to a query in a policy."""

    query_hash: str
    brittle_score: float = Field(ge=0.0, le=1.0)


class ArchivalPolicy(BaseModel):
    """
    Archival policy recommendation with confidence metrics.

    Represents an actionable recommendation for archiving specific table partitions
    with quantified risk and savings assessments.
    """

    policy_id: str = Field(default_factory=lambda: f"policy_{uuid.uuid4().hex[:8]}")
    table_id: str  # "database.schema.table"
    partition_key: str  # Column name
    partition_values: List[str]  # List of partition values to archive

    # Confidence metrics
    confidence_score: float = Field(ge=0.0, le=1.0)  # 1.0 = no brittle queries detected
    risk_score: float = Field(ge=0.0, le=1.0)  # 1.0 = high probability of breaking queries
    dependent_queries: List[QueryRef] = Field(default_factory=list)
    brittle_query_count: int = 0

    # Savings
    estimated_monthly_savings_usd: float = 0.0
    data_size_bytes: int = 0

    # Enforcement
    enforcement_action: EnforcementAction = EnforcementAction.DRY_RUN
    schedule_cron: Optional[str] = None  # e.g., "0 2 * * *" for 2 AM daily
    notification_channels: List[str] = Field(default_factory=list)  # Slack webhook URLs

    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None  # User ID
    approved_by: Optional[str] = None
    executed_at: Optional[datetime] = None
    tenant_id: Optional[str] = None  # For multi-tenancy

    @property
    def is_safe(self) -> bool:
        """Check if policy is safe to execute based on confidence score."""
        return self.confidence_score >= 0.9 and self.risk_score < 0.3

    @property
    def requires_approval(self) -> bool:
        """Check if policy requires manual approval."""
        return self.confidence_score < 0.9 or self.risk_score >= 0.3

    class Config:
        """Pydantic configuration."""

        json_encoders = {
            datetime: lambda v: v.isoformat(),
            EnforcementAction: str,
        }

