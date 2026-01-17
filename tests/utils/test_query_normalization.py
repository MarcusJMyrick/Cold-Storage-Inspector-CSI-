"""Tests for query normalization utilities."""

import pytest
from csi.utils.query_normalization import (
    normalize_query,
    compute_query_hash,
    detect_brittle_patterns,
    compute_brittle_score,
)


def test_normalize_query_basic():
    """Test basic query normalization."""
    query = "SELECT * FROM users WHERE id = 123"
    normalized = normalize_query(query)

    assert normalized == "select * from users where id = ?"
    assert "123" not in normalized  # Number should be masked


def test_normalize_query_string_literals():
    """Test string literal masking."""
    query = "SELECT * FROM users WHERE name = 'John'"
    normalized = normalize_query(query)

    assert normalized == "select * from users where name = ?"
    assert "John" not in normalized  # String should be masked


def test_normalize_query_whitespace():
    """Test whitespace normalization."""
    query = "SELECT    *   FROM\nusers\nWHERE\nid = 123"
    normalized = normalize_query(query)

    assert normalized == "select * from users where id = ?"
    assert "\n" not in normalized


def test_normalize_query_comments():
    """Test comment removal."""
    query = "SELECT * FROM users -- This is a comment\nWHERE id = 123"
    normalized = normalize_query(query)

    assert "--" not in normalized
    assert "comment" not in normalized


def test_normalize_query_multiline_comments():
    """Test multiline comment removal."""
    query = "SELECT * /* This is a\nmultiline comment */ FROM users"
    normalized = normalize_query(query)

    assert "/*" not in normalized
    assert "comment" not in normalized


def test_normalize_query_inner_join():
    """Test INNER JOIN canonicalization."""
    query = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
    normalized = normalize_query(query)

    assert "inner join" not in normalized.lower()
    assert "join" in normalized.lower()


def test_compute_query_hash_stability():
    """Test query hash stability across literal variations."""
    query1 = "SELECT * FROM users WHERE id = 123"
    query2 = "SELECT * FROM users WHERE id = 456"
    query3 = "SELECT * FROM users WHERE id = 999"

    hash1 = compute_query_hash(query1)
    hash2 = compute_query_hash(query2)
    hash3 = compute_query_hash(query3)

    # All should produce the same hash (literals are masked)
    assert hash1 == hash2 == hash3
    assert len(hash1) == 16  # Should be 16 characters


def test_compute_query_hash_different_queries():
    """Test that different queries produce different hashes."""
    query1 = "SELECT * FROM users"
    query2 = "SELECT * FROM orders"

    hash1 = compute_query_hash(query1)
    hash2 = compute_query_hash(query2)

    assert hash1 != hash2


def test_detect_brittle_patterns_select_star():
    """Test detection of SELECT * pattern."""
    query = "SELECT * FROM users WHERE id = 123"
    patterns = detect_brittle_patterns(query)

    assert len(patterns) >= 1
    pattern_names = [p[0] for p in patterns]
    assert "SELECT_STAR" in pattern_names


def test_detect_brittle_patterns_union_all():
    """Test detection of UNION ALL pattern."""
    query = "SELECT * FROM users UNION ALL SELECT * FROM orders"
    patterns = detect_brittle_patterns(query)

    pattern_names = [p[0] for p in patterns]
    assert "UNION_ALL" in pattern_names


def test_detect_brittle_patterns_full_outer_join():
    """Test detection of FULL OUTER JOIN pattern."""
    query = "SELECT * FROM users FULL OUTER JOIN orders ON users.id = orders.user_id"
    patterns = detect_brittle_patterns(query)

    pattern_names = [p[0] for p in patterns]
    assert "FULL_OUTER_JOIN" in pattern_names


def test_detect_brittle_patterns_materialized_view():
    """Test detection of materialized view pattern."""
    query = "CREATE MATERIALIZED VIEW mv_users AS SELECT * FROM users"
    patterns = detect_brittle_patterns(query)

    pattern_names = [p[0] for p in patterns]
    assert "MATERIALIZED_VIEW" in pattern_names


def test_compute_brittle_score_safe_query():
    """Test brittle score for safe query."""
    query = "SELECT name, email FROM users WHERE date >= '2024-01-01'"
    score = compute_brittle_score(query)

    # Should be low (0.0 or low risk)
    assert score < 0.5


def test_compute_brittle_score_risky_query():
    """Test brittle score for risky query."""
    query = "SELECT * FROM users FULL OUTER JOIN orders"
    score = compute_brittle_score(query)

    # Should be high (FULL OUTER JOIN has 0.9 risk)
    assert score >= 0.9


def test_compute_brittle_score_materialized_view():
    """Test brittle score for materialized view (highest risk)."""
    query = "CREATE MATERIALIZED VIEW mv_users AS SELECT * FROM users"
    score = compute_brittle_score(query)

    # Materialized views have risk score 1.0
    assert score == 1.0

