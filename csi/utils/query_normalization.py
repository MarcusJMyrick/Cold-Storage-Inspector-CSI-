"""Query normalization utilities for stable query hashing."""

import hashlib
import re
from typing import Optional


def normalize_query(text: str) -> str:
    """
    Normalize SQL query for stable hashing across literal variations.

    Steps:
    1. Remove comments (-- and /* */)
    2. Normalize whitespace (collapse to single spaces)
    3. Mask literals:
       - Numbers: `123` → `?`
       - Strings: `'abc'` → `?`
       - Booleans: `TRUE` → `?`
    4. Lowercase keywords (SELECT, FROM, WHERE)
    5. Canonicalize JOIN syntax (INNER JOIN → JOIN)

    Args:
        text: Raw SQL query text

    Returns:
        Normalized SQL query text

    Example:
        >>> normalize_query("SELECT * FROM users WHERE id = 123")
        'select * from users where id = ?'
    """
    if not text:
        return ""

    # Step 1: Remove SQL comments
    # Remove -- comments (single line)
    text = re.sub(r"--.*$", "", text, flags=re.MULTILINE)

    # Remove /* */ comments (multi-line)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)

    # Step 2: Normalize whitespace
    # Replace all whitespace sequences with single space
    text = re.sub(r"\s+", " ", text)
    # Remove leading/trailing whitespace
    text = text.strip()

    # Step 3: Mask literals
    # Mask string literals (single quotes)
    text = re.sub(r"'([^']|'')*'", "?", text)

    # Mask string literals (double quotes)
    text = re.sub(r'"([^"]|"")*"', "?", text)

    # Mask numeric literals (integers and decimals)
    # Negative lookbehind/lookahead to avoid matching identifiers
    text = re.sub(r"(?<![a-zA-Z0-9_$])-?\d+\.?\d*(?![a-zA-Z0-9_$])", "?", text)

    # Mask boolean literals
    text = re.sub(r"\b(TRUE|FALSE|true|false)\b", "?", text)

    # Mask NULL literals (optional - can keep as-is for some cases)
    # text = re.sub(r'\bNULL\b', '?', text, flags=re.IGNORECASE)

    # Step 4: Lowercase SQL keywords (preserve identifiers)
    # Common SQL keywords to lowercase
    keywords = [
        "SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER",
        "ON", "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "ILIKE",
        "GROUP", "BY", "HAVING", "ORDER", "LIMIT", "OFFSET",
        "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
        "CREATE", "DROP", "ALTER", "TABLE", "VIEW", "INDEX",
        "UNION", "INTERSECT", "EXCEPT", "ALL",
        "AS", "IS", "NULL",
    ]

    for keyword in keywords:
        # Use word boundaries to avoid matching substrings
        text = re.sub(rf"\b{keyword}\b", keyword.lower(), text, flags=re.IGNORECASE)

    # Step 5: Canonicalize JOIN syntax (INNER JOIN → JOIN)
    text = re.sub(r"\binner\s+join\b", "join", text, flags=re.IGNORECASE)

    # Final whitespace normalization
    text = re.sub(r"\s+", " ", text).strip()

    return text


def compute_query_hash(query_text: str, normalized: Optional[str] = None) -> str:
    """
    Compute deterministic hash for a query.

    Uses normalized query text to ensure same query with different literals
    produces the same hash.

    Args:
        query_text: Raw SQL query text
        normalized: Pre-normalized query text (optional, will normalize if not provided)

    Returns:
        16-character hexadecimal hash string

    Example:
        >>> compute_query_hash("SELECT * FROM users WHERE id = 123")
        'a1b2c3d4e5f6g7h8'
    """
    if normalized is None:
        normalized = normalize_query(query_text)

    # Use MD5 for fast hashing (32 hex chars)
    hash_obj = hashlib.md5(normalized.encode("utf-8"))
    hash_hex = hash_obj.hexdigest()

    # Return first 16 characters (64 bits of entropy is sufficient)
    return hash_hex[:16]


# Brittle pattern detection patterns
BRITTLE_PATTERNS = {
    "FULL_OUTER_JOIN": {
        "pattern": r"\bfull\s+outer\s+join\b",
        "description": "Archive would drop unmatched rows",
        "risk_score": 0.9,
    },
    "UNION_ALL": {
        "pattern": r"\bunion\s+all\b",
        "description": "Archive would reduce union cardinality",
        "risk_score": 0.9,
    },
    "SELECT_STAR": {
        "pattern": r"\bselect\s+\*\b",
        "description": "Archive might drop columns silently",
        "risk_score": 0.3,
    },
    "HARD_CODED_DATES": {
        "pattern": r"date\s*[=<>]+\s*\d{4}-\d{2}-\d{2}",
        "description": "Query assumes specific partitions exist",
        "risk_score": 0.6,
    },
    "NO_PARTITION_FILTER": {
        "pattern": r"where\s+(?!.*partition|.*date|.*timestamp)",  # Simplified
        "description": "Query scans entire table; archive would break",
        "risk_score": 0.6,
    },
    "MATERIALIZED_VIEW": {
        "pattern": r"\bcreate\s+materialized\s+view\b",
        "description": "MV depends on base table partitions",
        "risk_score": 1.0,
    },
    "EXTERNAL_TABLE": {
        "pattern": r"\bcreate\s+external\s+table\b",
        "description": "External table paths might reference archived partitions",
        "risk_score": 0.7,
    },
}


def detect_brittle_patterns(query_text: str, normalized: Optional[str] = None) -> list[tuple[str, float, str]]:
    """
    Detect brittle patterns in a query that would break if data is archived.

    Args:
        query_text: Raw SQL query text
        normalized: Pre-normalized query text (optional)

    Returns:
        List of tuples: (pattern_name, risk_score, description)

    Example:
        >>> detect_brittle_patterns("SELECT * FROM users")
        [('SELECT_STAR', 0.3, 'Archive might drop columns silently')]
    """
    if normalized is None:
        normalized = normalize_query(query_text)

    detected = []
    for pattern_name, pattern_info in BRITTLE_PATTERNS.items():
        if re.search(pattern_info["pattern"], normalized, re.IGNORECASE):
            detected.append((
                pattern_name,
                pattern_info["risk_score"],
                pattern_info["description"],
            ))

    return detected


def compute_brittle_score(query_text: str, normalized: Optional[str] = None) -> float:
    """
    Compute brittle score for a query.

    Returns the maximum risk score from detected brittle patterns.
    Returns 0.0 if query is partition-aware and safe.

    Args:
        query_text: Raw SQL query text
        normalized: Pre-normalized query text (optional)

    Returns:
        Float between 0.0 (safe) and 1.0 (certain break)

    Example:
        >>> compute_brittle_score("SELECT * FROM users WHERE date = '2024-01-01'")
        0.3  # SELECT * pattern
    """
    patterns = detect_brittle_patterns(query_text, normalized)

    if not patterns:
        return 0.0

    # Return maximum risk score
    return max(score for _, score, _ in patterns)

