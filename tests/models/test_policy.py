"""Tests for ArchivalPolicy model."""

import pytest
from csi.models.policy import ArchivalPolicy, QueryRef
from csi.models.enums import EnforcementAction


def test_archival_policy_creation():
    """Test creating an ArchivalPolicy."""
    policy = ArchivalPolicy(
        table_id="analytics.public.sales",
        partition_key="date",
        partition_values=["2024-01-01", "2024-01-02"],
        confidence_score=0.95,
        risk_score=0.05,
        estimated_monthly_savings_usd=1000.0,
        data_size_bytes=1024 * 1024 * 1024 * 10,  # 10 GB
    )
    
    assert policy.table_id == "analytics.public.sales"
    assert len(policy.partition_values) == 2
    assert policy.confidence_score == 0.95
    assert policy.policy_id is not None  # Auto-generated


def test_archival_policy_is_safe():
    """Test is_safe property."""
    # Safe policy
    safe_policy = ArchivalPolicy(
        table_id="test.table",
        partition_key="date",
        partition_values=["2024-01-01"],
        confidence_score=0.95,
        risk_score=0.1,
    )
    assert safe_policy.is_safe is True
    
    # Unsafe policy (low confidence)
    unsafe_policy = ArchivalPolicy(
        table_id="test.table",
        partition_key="date",
        partition_values=["2024-01-01"],
        confidence_score=0.7,
        risk_score=0.1,
    )
    assert unsafe_policy.is_safe is False
    
    # Unsafe policy (high risk)
    risky_policy = ArchivalPolicy(
        table_id="test.table",
        partition_key="date",
        partition_values=["2024-01-01"],
        confidence_score=0.95,
        risk_score=0.5,
    )
    assert risky_policy.is_safe is False


def test_archival_policy_requires_approval():
    """Test requires_approval property."""
    # Requires approval (low confidence)
    policy1 = ArchivalPolicy(
        table_id="test.table",
        partition_key="date",
        partition_values=["2024-01-01"],
        confidence_score=0.7,
        risk_score=0.1,
    )
    assert policy1.requires_approval is True
    
    # Requires approval (high risk)
    policy2 = ArchivalPolicy(
        table_id="test.table",
        partition_key="date",
        partition_values=["2024-01-01"],
        confidence_score=0.95,
        risk_score=0.5,
    )
    assert policy2.requires_approval is True
    
    # Does not require approval
    policy3 = ArchivalPolicy(
        table_id="test.table",
        partition_key="date",
        partition_values=["2024-01-01"],
        confidence_score=0.95,
        risk_score=0.1,
    )
    assert policy3.requires_approval is False


def test_archival_policy_dependent_queries():
    """Test dependent queries tracking."""
    policy = ArchivalPolicy(
        table_id="test.table",
        partition_key="date",
        partition_values=["2024-01-01"],
        dependent_queries=[
            QueryRef(query_hash="hash1", brittle_score=0.3),
            QueryRef(query_hash="hash2", brittle_score=0.7),
        ],
    )
    
    assert len(policy.dependent_queries) == 2
    assert policy.brittle_query_count == 0  # Set manually or computed elsewhere


def test_archival_policy_enforcement_actions():
    """Test enforcement action assignment."""
    # DRY_RUN (default)
    policy1 = ArchivalPolicy(
        table_id="test.table",
        partition_key="date",
        partition_values=["2024-01-01"],
    )
    assert policy1.enforcement_action == EnforcementAction.DRY_RUN
    
    # IMMEDIATE
    policy2 = ArchivalPolicy(
        table_id="test.table",
        partition_key="date",
        partition_values=["2024-01-01"],
        enforcement_action=EnforcementAction.IMMEDIATE,
    )
    assert policy2.enforcement_action == EnforcementAction.IMMEDIATE


def test_query_ref():
    """Test QueryRef model."""
    ref = QueryRef(query_hash="abc123", brittle_score=0.5)
    assert ref.query_hash == "abc123"
    assert ref.brittle_score == 0.5

