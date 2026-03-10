"""
Tool-Based Data Quality Agent.
ARCHITECTURE: LLM selects pre-built tools → Tools execute validated SQL → Return results.
ELIMINATES: All SQL syntax errors from LLM output.

v9 UPGRADES:
  - UNIVERSAL_TOOLS: null + empty check applied to EVERY column regardless of type
  - Massively expanded COLUMN_TOOLS library (80+ tools across all data types)
  - New tool categories: IP, URL, JSON, ZIP/Postal, Country, Currency, Percentage, Age
  - float_range_check added (was referenced but missing)
  - email_domain_distribution renamed to email_domain_check (was inconsistent)
"""
import json
import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from langgraph.graph import StateGraph, END

from app.agents.state import (
    AgentState, ValidationMode, AgentStatus,
    DataSourceInfo, ValidationResult, DataProfile
)
from app.agents.llm_service import get_llm_service
from app.agents.rag_service import get_rag_service
from app.agents.llm_sanitizer import sanitize_llm_response, validate_protocol
from app.connectors.factory import ConnectorFactory

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════
LLM_MAX_TOKENS = 2048
MAX_EXPLORATION_STEPS = 10
MAX_VALIDATION_STEPS = 50
GRAPH_RECURSION_LIMIT = 100
MAX_HISTORY_MESSAGES = 6
BATCH_SIZE = 5

# ═══════════════════════════════════════════════════════════════
# DATATYPE ENUM
# ═══════════════════════════════════════════════════════════════
class DataType(str, Enum):
    INTEGER    = "integer"
    STRING     = "string"
    EMAIL      = "email"
    PHONE      = "phone"
    DATE       = "date"
    DATETIME   = "datetime"
    FLOAT      = "float"
    BOOLEAN    = "boolean"
    CATEGORICAL= "categorical"
    UUID       = "uuid"
    TEXT       = "text"
    URL        = "url"
    IP         = "ip"
    JSON_COL   = "json_col"
    POSTAL     = "postal"
    COUNTRY    = "country"
    CURRENCY   = "currency"
    PERCENTAGE = "percentage"
    AGE        = "age"
    UNKNOWN    = "unknown"

# ═══════════════════════════════════════════════════════════════
# TOOL RESULT DATACLASS
# ═══════════════════════════════════════════════════════════════
@dataclass
class ToolResult:
    """Result from a validation tool execution."""
    tool_id: str
    tool_name: str
    command_executed: str
    status: str          # success, error, warning, skipped
    row_count: int
    failed_count: int
    sample_rows: List[Dict]
    message: str
    severity: str        # critical, warning, info
    column_name: Optional[str] = None
    execution_time_ms: int = 0


# ═══════════════════════════════════════════════════════════════
# UNIVERSAL TOOLS — applied to EVERY column regardless of type
# ═══════════════════════════════════════════════════════════════
# These run first, before any type-specific tools.
UNIVERSAL_TOOLS = {
    "universal_null_check": {
        "name": "NULL Check (Universal)",
        "description": "Count NULL values — applied to every column",
        "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
        "severity": "warning",
        "parameters": ["column"],
    },
    "universal_empty_check": {
        "name": "Empty / Blank Check (Universal)",
        "description": "Count empty string or whitespace-only values — applied to every column",
        "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM(CAST({column} AS TEXT)) = ''",
        "severity": "warning",
        "parameters": ["column"],
    },
    "universal_distinct_count": {
        "name": "Distinct Value Count (Universal)",
        "description": "Count of unique values — cardinality check for every column",
        "command": "SELECT COUNT(DISTINCT {column}) AS distinct_count FROM {table}",
        "severity": "info",
        "parameters": ["column"],
    },
    "universal_sample_values": {
        "name": "Top Sample Values (Universal)",
        "description": "Show top 10 most frequent values — applied to every column",
        "command": "SELECT {column}, COUNT(*) AS freq FROM {table} GROUP BY {column} ORDER BY freq DESC LIMIT 10",
        "severity": "info",
        "parameters": ["column"],
    },
    "universal_whitespace_padding": {
        "name": "Whitespace Padding Check (Universal)",
        "description": "Detect leading/trailing spaces on any column",
        "command": "SELECT COUNT(*) AS padded FROM {table} WHERE {column} IS NOT NULL AND TRIM(CAST({column} AS TEXT)) != CAST({column} AS TEXT)",
        "severity": "info",
        "parameters": ["column"],
    },
}

# ═══════════════════════════════════════════════════════════════
# TABLE-LEVEL TOOLS
# ═══════════════════════════════════════════════════════════════
TABLE_TOOLS = {
    "table_row_count": {
        "name": "Count Total Rows",
        "description": "Get total number of rows in the table",
        "command": "SELECT COUNT(*) AS total_rows FROM {table}",
        "category": "table_overview",
        "priority": 1,
        "parameters": [],
    },
    "table_null_scan": {
        "name": "Count Nulls in Column",
        "description": "Get count of NULL values for a specific column",
        "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
        "category": "table_overview",
        "priority": 2,
        "parameters": ["column"],
    },
    "table_duplicate_scan": {
        "name": "Find Duplicate Rows",
        "description": "Find rows that are completely duplicate across specified columns",
        "command": "SELECT {all_columns}, COUNT(*) AS dup_count FROM {table} GROUP BY {all_columns} HAVING COUNT(*) > 1",
        "category": "table_overview",
        "priority": 3,
        "parameters": [],
    },
    "table_sample_rows": {
        "name": "Get Sample Rows",
        "description": "Return first 10 rows for manual inspection",
        "command": "SELECT {all_columns} FROM {table} LIMIT 10",
        "category": "table_overview",
        "priority": 4,
        "parameters": [],
    },
    "table_empty_check": {
        "name": "Check If Table Is Empty",
        "description": "Verify table has data",
        "command": "SELECT CASE WHEN COUNT(*) = 0 THEN 'EMPTY' ELSE 'HAS_DATA' END AS status FROM {table}",
        "category": "table_overview",
        "priority": 5,
        "parameters": [],
    },
    "table_column_null_summary": {
        "name": "Null Summary Across All Columns",
        "description": "Return count of nulls for each column in one pass",
        "command": "SELECT {columns} FROM {table} LIMIT 1",
        "category": "table_overview",
        "priority": 6,
        "parameters": ["columns"],
    },
    "table_row_count_distinct": {
        "name": "Count Distinct Rows",
        "description": "Count fully distinct rows to compare vs total for duplicates",
        "command": "SELECT COUNT(*) AS distinct_rows FROM (SELECT DISTINCT * FROM {table})",
        "category": "table_overview",
        "priority": 7,
        "parameters": [],
    },
    "table_random_sample": {
        "name": "Random Sample Rows",
        "description": "Return 10 random rows for unbiased inspection",
        "command": "SELECT {all_columns} FROM {table} ORDER BY RANDOM() LIMIT 10",
        "category": "table_overview",
        "priority": 8,
        "parameters": [],
    },
}

# ═══════════════════════════════════════════════════════════════
# COLUMN-LEVEL TOOLS (type-specific — on top of universal tools)
# ═══════════════════════════════════════════════════════════════
COLUMN_TOOLS = {

    # ══════════════════════════════════════════
    # INTEGER / ID COLUMNS
    # ══════════════════════════════════════════
    DataType.INTEGER: {
        "int_null_check": {
            "name": "Check NULL Values",
            "description": "Count rows where column is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "int_negative_check": {
            "name": "Check Negative Values",
            "description": "Find negative values (invalid for IDs or counts)",
            "command": "SELECT COUNT(*) AS negative_count FROM {table} WHERE {column} < 0",
            "severity": "critical",
            "parameters": ["column"],
        },
        "int_zero_check": {
            "name": "Check Zero Values",
            "description": "Find zero values (may be invalid for IDs)",
            "command": "SELECT COUNT(*) AS zero_count FROM {table} WHERE {column} = 0",
            "severity": "warning",
            "parameters": ["column"],
        },
        "int_uniqueness_check": {
            "name": "Check Uniqueness",
            "description": "Count duplicate values",
            "command": "SELECT {column}, COUNT(*) AS cnt FROM {table} GROUP BY {column} HAVING COUNT(*) > 1",
            "severity": "critical",
            "parameters": ["column"],
        },
        "int_range_check": {
            "name": "Check Min/Max Range",
            "description": "Get min and max values",
            "command": "SELECT MIN({column}) AS min_val, MAX({column}) AS max_val, AVG({column}) AS avg_val FROM {table}",
            "severity": "info",
            "parameters": ["column"],
        },
        "int_stddev_check": {
            "name": "Check Standard Deviation",
            "description": "Compute standard deviation to identify spread anomalies",
            "command": "SELECT AVG({column}) AS mean, (AVG({column} * {column}) - AVG({column}) * AVG({column})) AS variance FROM {table} WHERE {column} IS NOT NULL",
            "severity": "info",
            "parameters": ["column"],
        },
        "int_extreme_outlier_check": {
            "name": "Check Extreme Outliers",
            "description": "Find values more than 10x the average",
            "command": "SELECT COUNT(*) AS outlier_count FROM {table} CROSS JOIN (SELECT AVG({column}) AS avg_v FROM {table} WHERE {column} > 0) s WHERE {column} > s.avg_v * 10 AND {column} > 0",
            "severity": "warning",
            "parameters": ["column"],
        },
        "int_suspicious_seq_check": {
            "name": "Check for Suspicious Round Numbers",
            "description": "Find values that are suspiciously round (multiples of 1000) — may indicate placeholder data",
            "command": "SELECT COUNT(*) AS round_count FROM {table} WHERE {column} % 1000 = 0 AND {column} > 0",
            "severity": "info",
            "parameters": ["column"],
        },
        "int_pk_gap_check": {
            "name": "Check ID Sequence Gaps",
            "description": "Find gaps in an integer ID sequence (count expected vs actual)",
            "command": "SELECT (MAX({column}) - MIN({column}) + 1) - COUNT(DISTINCT {column}) AS gap_count FROM {table} WHERE {column} IS NOT NULL",
            "severity": "info",
            "parameters": ["column"],
        },
        "int_distribution_check": {
            "name": "Check Value Distribution",
            "description": "Show top 20 most common integer values",
            "command": "SELECT {column}, COUNT(*) AS freq FROM {table} GROUP BY {column} ORDER BY freq DESC LIMIT 20",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # FLOAT / DECIMAL COLUMNS
    # ══════════════════════════════════════════
    DataType.FLOAT: {
        "float_null_check": {
            "name": "Check NULL Values",
            "description": "Count rows where column is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "float_negative_check": {
            "name": "Check Negative Values",
            "description": "Find negative values (may be invalid for prices/amounts)",
            "command": "SELECT COUNT(*) AS negative_count FROM {table} WHERE {column} < 0",
            "severity": "warning",
            "parameters": ["column"],
        },
        "float_zero_check": {
            "name": "Check Zero Values",
            "description": "Find zero values (may indicate missing data)",
            "command": "SELECT COUNT(*) AS zero_count FROM {table} WHERE {column} = 0",
            "severity": "info",
            "parameters": ["column"],
        },
        "float_range_check": {
            "name": "Check Min/Max/Avg Range",
            "description": "Get min, max, and average for range analysis",
            "command": "SELECT MIN({column}) AS min_val, MAX({column}) AS max_val, AVG({column}) AS avg_val FROM {table} WHERE {column} IS NOT NULL",
            "severity": "info",
            "parameters": ["column"],
        },
        "float_precision_check": {
            "name": "Check Decimal Precision",
            "description": "Find values with unexpected extra decimal places",
            "command": "SELECT COUNT(*) AS precision_issue FROM {table} WHERE {column} IS NOT NULL AND LENGTH(SUBSTR(CAST({column} AS TEXT), INSTR(CAST({column} AS TEXT), '.') + 1)) > {max_decimals}",
            "severity": "info",
            "parameters": ["column", "max_decimals"],
        },
        "float_nan_check": {
            "name": "Check NaN / Infinity Values",
            "description": "Detect NaN or Infinity stored as text in numeric column",
            "command": "SELECT COUNT(*) AS nan_inf_count FROM {table} WHERE LOWER(CAST({column} AS TEXT)) IN ('nan', 'inf', '-inf', 'infinity', '-infinity')",
            "severity": "critical",
            "parameters": ["column"],
        },
        "float_extreme_outlier_check": {
            "name": "Check Extreme Outliers (>3σ)",
            "description": "Find values beyond 3 standard deviations using variance formula",
            "command": "SELECT COUNT(*) AS outlier_count FROM {table} CROSS JOIN (SELECT AVG({column}) AS mu, (AVG({column}*{column}) - AVG({column})*AVG({column})) AS var2 FROM {table} WHERE {column} IS NOT NULL) s WHERE ABS({column} - s.mu) > 3 * SQRT(MAX(s.var2, 0.0001))",
            "severity": "warning",
            "parameters": ["column"],
        },
        "float_negative_amount_check": {
            "name": "Check Suspicious Negative Amounts",
            "description": "Find negative amounts that may represent refunds or errors",
            "command": "SELECT MIN({column}) AS min_val, COUNT(*) AS neg_count FROM {table} WHERE {column} < -0.01",
            "severity": "warning",
            "parameters": ["column"],
        },
        "float_round_number_check": {
            "name": "Check Suspiciously Round Values",
            "description": "Find integer-valued floats — may indicate rounding or placeholder data",
            "command": "SELECT COUNT(*) AS round_count FROM {table} WHERE {column} IS NOT NULL AND {column} = CAST({column} AS INTEGER)",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # STRING / TEXT COLUMNS
    # ══════════════════════════════════════════
    DataType.STRING: {
        "str_null_check": {
            "name": "Check NULL Values",
            "description": "Count rows where column is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "str_empty_check": {
            "name": "Check Empty Strings",
            "description": "Find empty or whitespace-only values",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE TRIM({column}) = ''",
            "severity": "warning",
            "parameters": ["column"],
        },
        "str_whitespace_padding": {
            "name": "Check Whitespace Padding",
            "description": "Detect leading or trailing spaces",
            "command": "SELECT COUNT(*) AS padded FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) != {column}",
            "severity": "info",
            "parameters": ["column"],
        },
        "str_length_check": {
            "name": "Check Max Length Violations",
            "description": "Find values exceeding expected max length",
            "command": "SELECT COUNT(*) AS long_count FROM {table} WHERE LENGTH({column}) > {max_length}",
            "severity": "warning",
            "parameters": ["column", "max_length"],
        },
        "str_min_length_check": {
            "name": "Check Minimum Length",
            "description": "Find suspiciously short values (1 character)",
            "command": "SELECT COUNT(*) AS too_short FROM {table} WHERE {column} IS NOT NULL AND LENGTH(TRIM({column})) = 1",
            "severity": "warning",
            "parameters": ["column"],
        },
        "str_special_char_check": {
            "name": "Check Special Characters",
            "description": "Find values with unexpected special characters",
            "command": "SELECT COUNT(*) AS special_count FROM {table} WHERE {column} GLOB '*[!a-zA-Z0-9 .,_-]*'",
            "severity": "info",
            "parameters": ["column"],
        },
        "str_control_char_check": {
            "name": "Check Control Characters",
            "description": "Detect tab, newline, carriage-return characters inside values",
            "command": "SELECT COUNT(*) AS ctrl_count FROM {table} WHERE {column} LIKE '%' || CHAR(9) || '%' OR {column} LIKE '%' || CHAR(10) || '%' OR {column} LIKE '%' || CHAR(13) || '%'",
            "severity": "warning",
            "parameters": ["column"],
        },
        "str_sample_values": {
            "name": "Get Distinct Sample Values",
            "description": "Show top 20 most common values",
            "command": "SELECT {column}, COUNT(*) AS cnt FROM {table} GROUP BY {column} ORDER BY cnt DESC LIMIT 20",
            "severity": "info",
            "parameters": ["column"],
        },
        "str_distinct_count": {
            "name": "Count Distinct Values",
            "description": "Cardinality check for string columns",
            "command": "SELECT COUNT(DISTINCT {column}) AS distinct_count FROM {table}",
            "severity": "info",
            "parameters": ["column"],
        },
        "str_numeric_stored_as_text": {
            "name": "Check Numerics Stored as Text",
            "description": "Detect columns that are numeric but stored as string",
            "command": "SELECT COUNT(*) AS numeric_text FROM {table} WHERE {column} IS NOT NULL AND {column} NOT GLOB '*[!0-9.+-]*' AND TRIM({column}) != ''",
            "severity": "info",
            "parameters": ["column"],
        },
        "str_placeholder_check": {
            "name": "Check Placeholder Values",
            "description": "Detect common placeholder values (N/A, NULL, none, TBD, test, unknown)",
            "command": "SELECT COUNT(*) AS placeholder_count FROM {table} WHERE LOWER(TRIM({column})) IN ('n/a', 'null', 'none', 'tbd', 'test', 'unknown', 'na', '?', '-', 'placeholder', 'undefined', 'missing')",
            "severity": "warning",
            "parameters": ["column"],
        },
        "str_mixed_case_check": {
            "name": "Check Mixed Case Inconsistency",
            "description": "Detect same value stored in different cases (e.g. 'Active' vs 'active')",
            "command": "SELECT LOWER({column}) AS val_lower, COUNT(DISTINCT {column}) AS case_variants FROM {table} WHERE {column} IS NOT NULL GROUP BY LOWER({column}) HAVING COUNT(DISTINCT {column}) > 1 LIMIT 10",
            "severity": "warning",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # EMAIL COLUMNS
    # ══════════════════════════════════════════
    DataType.EMAIL: {
        "email_null_check": {
            "name": "Check NULL Emails",
            "description": "Count rows where email is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "email_empty_check": {
            "name": "Check Empty Email Strings",
            "description": "Find empty or whitespace-only email values",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) = ''",
            "severity": "critical",
            "parameters": ["column"],
        },
        "email_format_check": {
            "name": "Check Invalid Email Format",
            "description": "Find emails not matching basic pattern (must contain @domain.tld)",
            "command": "SELECT COUNT(*) AS invalid_count FROM {table} WHERE {column} IS NOT NULL AND {column} NOT GLOB '*@*.*'",
            "severity": "critical",
            "parameters": ["column"],
        },
        "email_no_at_check": {
            "name": "Check Missing @ Symbol",
            "description": "Find emails with no @ at all",
            "command": "SELECT COUNT(*) AS no_at FROM {table} WHERE {column} IS NOT NULL AND INSTR({column}, '@') = 0",
            "severity": "critical",
            "parameters": ["column"],
        },
        "email_multiple_at_check": {
            "name": "Check Multiple @ Symbols",
            "description": "Find emails with more than one @ symbol",
            "command": "SELECT COUNT(*) AS multi_at FROM {table} WHERE {column} IS NOT NULL AND LENGTH({column}) - LENGTH(REPLACE({column}, '@', '')) > 1",
            "severity": "critical",
            "parameters": ["column"],
        },
        "email_duplicate_check": {
            "name": "Check Duplicate Emails",
            "description": "Find duplicate email addresses (case-insensitive)",
            "command": "SELECT LOWER({column}) AS email_lower, COUNT(*) AS cnt FROM {table} GROUP BY LOWER({column}) HAVING COUNT(*) > 1",
            "severity": "warning",
            "parameters": ["column"],
        },
        "email_domain_check": {
            "name": "Check Email Domain Distribution",
            "description": "Show top email domains by frequency",
            "command": "SELECT SUBSTR({column}, INSTR({column}, '@') + 1) AS domain, COUNT(*) AS cnt FROM {table} WHERE {column} IS NOT NULL GROUP BY domain ORDER BY cnt DESC LIMIT 15",
            "severity": "info",
            "parameters": ["column"],
        },
        "email_typo_check": {
            "name": "Check Common Domain Typos",
            "description": "Detect common misspellings: gmil, yaho, hotmial, etc.",
            "command": "SELECT COUNT(*) AS typo_count FROM {table} WHERE {column} GLOB '*@gmil.*' OR {column} GLOB '*@yaho.*' OR {column} GLOB '*@hotmial.*' OR {column} GLOB '*@gmal.*' OR {column} GLOB '*@outlok.*'",
            "severity": "warning",
            "parameters": ["column"],
        },
        "email_test_domain_check": {
            "name": "Check Test / Placeholder Emails",
            "description": "Detect test or fake email domains",
            "command": "SELECT COUNT(*) AS test_count FROM {table} WHERE {column} LIKE '%@test.%' OR {column} LIKE '%@example.%' OR {column} LIKE '%@placeholder.%' OR {column} LIKE '%@fake.%' OR {column} LIKE '%@mailinator.%'",
            "severity": "warning",
            "parameters": ["column"],
        },
        "email_length_check": {
            "name": "Check Email Length",
            "description": "Find email addresses that are unreasonably short or long",
            "command": "SELECT COUNT(*) AS bad_length FROM {table} WHERE {column} IS NOT NULL AND (LENGTH({column}) < 6 OR LENGTH({column}) > 254)",
            "severity": "warning",
            "parameters": ["column"],
        },
        "email_uppercase_check": {
            "name": "Check Uppercase Emails",
            "description": "Find emails with uppercase characters (should be normalised to lowercase)",
            "command": "SELECT COUNT(*) AS upper_count FROM {table} WHERE {column} IS NOT NULL AND {column} != LOWER({column})",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # PHONE COLUMNS
    # ══════════════════════════════════════════
    DataType.PHONE: {
        "phone_null_check": {
            "name": "Check NULL Phones",
            "description": "Count rows where phone is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "phone_empty_check": {
            "name": "Check Empty Phone Strings",
            "description": "Find empty or whitespace-only phone values",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) = ''",
            "severity": "critical",
            "parameters": ["column"],
        },
        "phone_format_check": {
            "name": "Check Invalid Phone Format",
            "description": "Find phones with non-numeric characters (except +, -, spaces, parentheses)",
            "command": "SELECT COUNT(*) AS invalid_count FROM {table} WHERE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE({column}, '+', ''), '-', ''), ' ', ''), '(', ''), ')', '') GLOB '*[!0-9]*'",
            "severity": "critical",
            "parameters": ["column"],
        },
        "phone_length_check": {
            "name": "Check Phone Length",
            "description": "Find phones with invalid digit count (must be 7-15 digits)",
            "command": "SELECT COUNT(*) AS invalid_length FROM {table} WHERE LENGTH(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE({column}, '+', ''), '-', ''), ' ', ''), '(', ''), ')', '')) NOT BETWEEN 7 AND 15",
            "severity": "warning",
            "parameters": ["column"],
        },
        "phone_placeholder_check": {
            "name": "Check Placeholder Phone Values",
            "description": "Find test or placeholder values like 000, N/A, NULL",
            "command": "SELECT COUNT(*) AS placeholder_count FROM {table} WHERE LOWER(TRIM({column})) IN ('0', '000', '0000', '00000', 'n/a', 'null', 'none', 'na', 'test', '1234567890', '9999999999')",
            "severity": "warning",
            "parameters": ["column"],
        },
        "phone_country_code_check": {
            "name": "Check Country Code Distribution",
            "description": "Show distribution of leading country codes",
            "command": "SELECT SUBSTR({column}, 1, 3) AS code_prefix, COUNT(*) AS cnt FROM {table} WHERE {column} LIKE '+%' GROUP BY code_prefix ORDER BY cnt DESC LIMIT 10",
            "severity": "info",
            "parameters": ["column"],
        },
        "phone_all_same_check": {
            "name": "Check Suspiciously Uniform Values",
            "description": "Detect if a single phone number appears more than 1% of the time",
            "command": "SELECT {column}, COUNT(*) AS cnt FROM {table} GROUP BY {column} ORDER BY cnt DESC LIMIT 5",
            "severity": "warning",
            "parameters": ["column"],
        },
        "phone_local_only_check": {
            "name": "Check Local Numbers Without Country Code",
            "description": "Find numbers that don't start with + (no country code)",
            "command": "SELECT COUNT(*) AS no_country_code FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) != '' AND {column} NOT LIKE '+%'",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # DATE COLUMNS
    # ══════════════════════════════════════════
    DataType.DATE: {
        "date_null_check": {
            "name": "Check NULL Dates",
            "description": "Count rows where date is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "date_empty_check": {
            "name": "Check Empty Date Strings",
            "description": "Find empty string dates",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM(CAST({column} AS TEXT)) = ''",
            "severity": "critical",
            "parameters": ["column"],
        },
        "date_future_check": {
            "name": "Check Future Dates",
            "description": "Find dates set in the future",
            "command": "SELECT COUNT(*) AS future_count FROM {table} WHERE {column} > DATE('now')",
            "severity": "critical",
            "parameters": ["column"],
        },
        "date_past_check": {
            "name": "Check Too Old Dates (pre-1900)",
            "description": "Find dates before 1900 — likely parse errors or defaults",
            "command": "SELECT COUNT(*) AS old_count FROM {table} WHERE {column} < '1900-01-01'",
            "severity": "warning",
            "parameters": ["column"],
        },
        "date_format_check": {
            "name": "Check Date Format (YYYY-MM-DD)",
            "description": "Find values not matching ISO date format",
            "command": "SELECT COUNT(*) AS invalid_format FROM {table} WHERE {column} IS NOT NULL AND {column} NOT GLOB '????-??-??'",
            "severity": "critical",
            "parameters": ["column"],
        },
        "date_range_check": {
            "name": "Check Date Range",
            "description": "Get min and max dates for range plausibility",
            "command": "SELECT MIN({column}) AS min_date, MAX({column}) AS max_date FROM {table} WHERE {column} IS NOT NULL",
            "severity": "info",
            "parameters": ["column"],
        },
        "date_unix_epoch_check": {
            "name": "Check Unix Epoch Default (1970-01-01)",
            "description": "Find dates equal to the Unix epoch — indicates date parsing failure",
            "command": "SELECT COUNT(*) AS epoch_count FROM {table} WHERE CAST({column} AS TEXT) LIKE '1970-01-01%'",
            "severity": "warning",
            "parameters": ["column"],
        },
        "date_day_distribution": {
            "name": "Check Day-of-Week Distribution",
            "description": "Show count of dates by day name — detect weekend bias",
            "command": "SELECT STRFTIME('%w', {column}) AS weekday, COUNT(*) AS cnt FROM {table} WHERE {column} IS NOT NULL GROUP BY weekday ORDER BY weekday",
            "severity": "info",
            "parameters": ["column"],
        },
        "date_month_distribution": {
            "name": "Check Month Distribution",
            "description": "Show count of dates per month — detect bias or missing months",
            "command": "SELECT STRFTIME('%m', {column}) AS month, COUNT(*) AS cnt FROM {table} WHERE {column} IS NOT NULL GROUP BY month ORDER BY month",
            "severity": "info",
            "parameters": ["column"],
        },
        "date_invalid_day_check": {
            "name": "Check Invalid Day (00)",
            "description": "Find dates with day component = 00 (e.g. 2023-05-00)",
            "command": "SELECT COUNT(*) AS bad_day FROM {table} WHERE {column} IS NOT NULL AND STRFTIME('%d', {column}) = '00'",
            "severity": "critical",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # DATETIME / TIMESTAMP COLUMNS
    # ══════════════════════════════════════════
    DataType.DATETIME: {
        "datetime_null_check": {
            "name": "Check NULL Datetimes",
            "description": "Count rows where datetime is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "datetime_empty_check": {
            "name": "Check Empty Datetime Strings",
            "description": "Find empty datetime values",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM(CAST({column} AS TEXT)) = ''",
            "severity": "critical",
            "parameters": ["column"],
        },
        "datetime_future_check": {
            "name": "Check Future Datetimes",
            "description": "Find datetimes in the future",
            "command": "SELECT COUNT(*) AS future_count FROM {table} WHERE {column} > DATETIME('now')",
            "severity": "critical",
            "parameters": ["column"],
        },
        "datetime_format_check": {
            "name": "Check Datetime Format",
            "description": "Find values not matching YYYY-MM-DD HH:MM:SS",
            "command": "SELECT COUNT(*) AS invalid_format FROM {table} WHERE {column} IS NOT NULL AND {column} NOT GLOB '????-??-?? ??:??:??*'",
            "severity": "critical",
            "parameters": ["column"],
        },
        "datetime_timezone_check": {
            "name": "Check Mixed Timezone Markers",
            "description": "Detect values with timezone suffixes — mixed TZ is problematic",
            "command": "SELECT COUNT(*) AS tz_count FROM {table} WHERE {column} LIKE '%+%:%' OR {column} LIKE '%Z' OR {column} LIKE '%-05:00'",
            "severity": "info",
            "parameters": ["column"],
        },
        "datetime_epoch_check": {
            "name": "Check Unix Epoch Default",
            "description": "Find timestamps at 1970-01-01 — date parsing default",
            "command": "SELECT COUNT(*) AS epoch_count FROM {table} WHERE CAST({column} AS TEXT) LIKE '1970-01-01%'",
            "severity": "warning",
            "parameters": ["column"],
        },
        "datetime_range_check": {
            "name": "Check Datetime Range",
            "description": "Get min and max datetime values",
            "command": "SELECT MIN({column}) AS min_dt, MAX({column}) AS max_dt FROM {table} WHERE {column} IS NOT NULL",
            "severity": "info",
            "parameters": ["column"],
        },
        "datetime_sample": {
            "name": "Sample Distinct Datetime Values",
            "description": "Show 10 distinct datetime values for inspection",
            "command": "SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL LIMIT 10",
            "severity": "info",
            "parameters": ["column"],
        },
        "datetime_midnight_check": {
            "name": "Check Suspiciously Many Midnights",
            "description": "Detect if most timestamps have time = 00:00:00 (may mean time was dropped)",
            "command": "SELECT COUNT(*) AS midnight_count FROM {table} WHERE {column} IS NOT NULL AND STRFTIME('%H:%M:%S', {column}) = '00:00:00'",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # BOOLEAN COLUMNS
    # ══════════════════════════════════════════
    DataType.BOOLEAN: {
        "bool_null_check": {
            "name": "Check NULL Booleans",
            "description": "Count rows where boolean column is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "bool_empty_check": {
            "name": "Check Empty Boolean Strings",
            "description": "Find empty string values in boolean column",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM(CAST({column} AS TEXT)) = ''",
            "severity": "critical",
            "parameters": ["column"],
        },
        "bool_invalid_check": {
            "name": "Check Invalid Boolean Values",
            "description": "Find values not in (0, 1, TRUE, FALSE, true, false)",
            "command": "SELECT COUNT(*) AS invalid_count FROM {table} WHERE {column} IS NOT NULL AND LOWER(CAST({column} AS TEXT)) NOT IN ('0', '1', 'true', 'false', 't', 'f', 'yes', 'no')",
            "severity": "critical",
            "parameters": ["column"],
        },
        "bool_distribution_check": {
            "name": "Check Boolean Distribution",
            "description": "Show TRUE/FALSE count and ratio",
            "command": "SELECT LOWER(CAST({column} AS TEXT)) AS val, COUNT(*) AS cnt FROM {table} GROUP BY LOWER(CAST({column} AS TEXT))",
            "severity": "info",
            "parameters": ["column"],
        },
        "bool_all_same_check": {
            "name": "Check All-Same Boolean Value",
            "description": "Detect if column has only one distinct value (no variation)",
            "command": "SELECT COUNT(DISTINCT {column}) AS distinct_bool FROM {table} WHERE {column} IS NOT NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # CATEGORICAL / ENUM / STATUS COLUMNS
    # ══════════════════════════════════════════
    DataType.CATEGORICAL: {
        "cat_null_check": {
            "name": "Check NULL Values",
            "description": "Count rows where column is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "cat_empty_check": {
            "name": "Check Empty Category Strings",
            "description": "Find empty or whitespace values",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) = ''",
            "severity": "critical",
            "parameters": ["column"],
        },
        "cat_invalid_check": {
            "name": "Check Invalid Categories",
            "description": "Find values not in expected set (provide expected_values)",
            "command": "SELECT COUNT(*) AS invalid_count FROM {table} WHERE {column} NOT IN ({expected_values})",
            "severity": "critical",
            "parameters": ["column", "expected_values"],
        },
        "cat_distribution_check": {
            "name": "Check Category Distribution",
            "description": "Show all distinct category values with row counts",
            "command": "SELECT {column}, COUNT(*) AS cnt FROM {table} GROUP BY {column} ORDER BY cnt DESC",
            "severity": "info",
            "parameters": ["column"],
        },
        "cat_rare_check": {
            "name": "Check Rare Categories",
            "description": "Find categories occurring fewer than 5 times — may be typos",
            "command": "SELECT {column}, COUNT(*) AS cnt FROM {table} GROUP BY {column} HAVING COUNT(*) < 5 ORDER BY cnt",
            "severity": "warning",
            "parameters": ["column"],
        },
        "cat_mixed_case_check": {
            "name": "Check Mixed Case Variants",
            "description": "Detect same category stored in different casing",
            "command": "SELECT LOWER({column}) AS lower_val, COUNT(DISTINCT {column}) AS variants FROM {table} WHERE {column} IS NOT NULL GROUP BY LOWER({column}) HAVING COUNT(DISTINCT {column}) > 1",
            "severity": "warning",
            "parameters": ["column"],
        },
        "cat_deprecated_check": {
            "name": "Check Deprecated Category Values",
            "description": "Find values that should no longer appear (provide deprecated_values)",
            "command": "SELECT COUNT(*) AS deprecated_count FROM {table} WHERE {column} IN ({deprecated_values})",
            "severity": "warning",
            "parameters": ["column", "deprecated_values"],
        },
        "cat_cardinality_check": {
            "name": "Check Cardinality",
            "description": "Count distinct values — high cardinality in a categorical column is suspicious",
            "command": "SELECT COUNT(DISTINCT {column}) AS distinct_count, COUNT(*) AS total FROM {table}",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # UUID / GUID COLUMNS
    # ══════════════════════════════════════════
    DataType.UUID: {
        "uuid_null_check": {
            "name": "Check NULL UUIDs",
            "description": "Count rows where UUID is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "uuid_empty_check": {
            "name": "Check Empty UUID Strings",
            "description": "Find empty UUID values",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) = ''",
            "severity": "critical",
            "parameters": ["column"],
        },
        "uuid_format_check": {
            "name": "Check UUID Format",
            "description": "Find UUIDs not matching 8-4-4-4-12 hex format",
            "command": "SELECT COUNT(*) AS invalid_count FROM {table} WHERE {column} IS NOT NULL AND {column} NOT GLOB '????????-????-????-????-????????????'",
            "severity": "critical",
            "parameters": ["column"],
        },
        "uuid_uniqueness_check": {
            "name": "Check UUID Uniqueness",
            "description": "Find duplicate UUIDs — must be globally unique",
            "command": "SELECT {column}, COUNT(*) AS cnt FROM {table} GROUP BY {column} HAVING COUNT(*) > 1",
            "severity": "critical",
            "parameters": ["column"],
        },
        "uuid_uppercase_check": {
            "name": "Check UUID Case Consistency",
            "description": "Find mixed-case UUIDs (should all be lowercase or all uppercase)",
            "command": "SELECT COUNT(*) AS mixed_case FROM {table} WHERE {column} IS NOT NULL AND {column} != LOWER({column}) AND {column} != UPPER({column})",
            "severity": "info",
            "parameters": ["column"],
        },
        "uuid_nil_check": {
            "name": "Check Nil UUID (all zeros)",
            "description": "Find nil UUIDs (00000000-0000-0000-0000-000000000000) used as placeholders",
            "command": "SELECT COUNT(*) AS nil_count FROM {table} WHERE {column} = '00000000-0000-0000-0000-000000000000'",
            "severity": "warning",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # URL COLUMNS
    # ══════════════════════════════════════════
    DataType.URL: {
        "url_null_check": {
            "name": "Check NULL URLs",
            "description": "Count rows where URL is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "url_empty_check": {
            "name": "Check Empty URL Strings",
            "description": "Find empty URL values",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) = ''",
            "severity": "critical",
            "parameters": ["column"],
        },
        "url_format_check": {
            "name": "Check URL Format",
            "description": "Find URLs not starting with http:// or https://",
            "command": "SELECT COUNT(*) AS invalid_url FROM {table} WHERE {column} IS NOT NULL AND {column} NOT LIKE 'http://%' AND {column} NOT LIKE 'https://%'",
            "severity": "critical",
            "parameters": ["column"],
        },
        "url_http_check": {
            "name": "Check Insecure HTTP URLs",
            "description": "Find plain http:// URLs (not https)",
            "command": "SELECT COUNT(*) AS insecure_count FROM {table} WHERE {column} LIKE 'http://%' AND {column} NOT LIKE 'https://%'",
            "severity": "info",
            "parameters": ["column"],
        },
        "url_localhost_check": {
            "name": "Check Localhost / Dev URLs",
            "description": "Find test or dev URLs pointing to localhost or 127.0.0.1",
            "command": "SELECT COUNT(*) AS localhost_count FROM {table} WHERE {column} LIKE '%localhost%' OR {column} LIKE '%127.0.0.1%' OR {column} LIKE '%0.0.0.0%'",
            "severity": "warning",
            "parameters": ["column"],
        },
        "url_length_check": {
            "name": "Check URL Length",
            "description": "Find unreasonably long URLs (> 2048 chars)",
            "command": "SELECT COUNT(*) AS long_url FROM {table} WHERE LENGTH({column}) > 2048",
            "severity": "warning",
            "parameters": ["column"],
        },
        "url_sample_check": {
            "name": "Sample URL Values",
            "description": "Show sample URLs for manual inspection",
            "command": "SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL LIMIT 10",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # IP ADDRESS COLUMNS
    # ══════════════════════════════════════════
    DataType.IP: {
        "ip_null_check": {
            "name": "Check NULL IP Addresses",
            "description": "Count rows where IP is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "ip_empty_check": {
            "name": "Check Empty IP Strings",
            "description": "Find empty IP address values",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) = ''",
            "severity": "critical",
            "parameters": ["column"],
        },
        "ip_format_check": {
            "name": "Check Basic IPv4 Format",
            "description": "Find IPs not matching basic N.N.N.N pattern",
            "command": "SELECT COUNT(*) AS invalid_ip FROM {table} WHERE {column} IS NOT NULL AND {column} NOT GLOB '[0-9]*.[0-9]*.[0-9]*.[0-9]*'",
            "severity": "critical",
            "parameters": ["column"],
        },
        "ip_private_check": {
            "name": "Check Private IP Ranges",
            "description": "Detect private/internal IPs (10.x, 192.168.x, 172.16.x)",
            "command": "SELECT COUNT(*) AS private_count FROM {table} WHERE {column} LIKE '10.%' OR {column} LIKE '192.168.%' OR {column} LIKE '172.16.%' OR {column} LIKE '172.17.%' OR {column} LIKE '172.18.%'",
            "severity": "info",
            "parameters": ["column"],
        },
        "ip_loopback_check": {
            "name": "Check Loopback IPs",
            "description": "Find loopback/localhost IPs (127.0.0.1)",
            "command": "SELECT COUNT(*) AS loopback_count FROM {table} WHERE {column} LIKE '127.%'",
            "severity": "warning",
            "parameters": ["column"],
        },
        "ip_distribution": {
            "name": "Check IP Distribution",
            "description": "Show top 10 most frequent IPs",
            "command": "SELECT {column}, COUNT(*) AS cnt FROM {table} GROUP BY {column} ORDER BY cnt DESC LIMIT 10",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # ZIP / POSTAL CODE COLUMNS
    # ══════════════════════════════════════════
    DataType.POSTAL: {
        "postal_null_check": {
            "name": "Check NULL Postal Codes",
            "description": "Count rows where postal code is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "postal_empty_check": {
            "name": "Check Empty Postal Codes",
            "description": "Find empty postal code values",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) = ''",
            "severity": "critical",
            "parameters": ["column"],
        },
        "postal_us_format_check": {
            "name": "Check US ZIP Code Format",
            "description": "Find US ZIPs not matching 5-digit or 5+4 format",
            "command": "SELECT COUNT(*) AS invalid_zip FROM {table} WHERE {column} IS NOT NULL AND {column} NOT GLOB '[0-9][0-9][0-9][0-9][0-9]' AND {column} NOT GLOB '[0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'",
            "severity": "warning",
            "parameters": ["column"],
        },
        "postal_length_check": {
            "name": "Check Postal Code Length",
            "description": "Find postal codes outside expected length range (3-10 chars)",
            "command": "SELECT COUNT(*) AS bad_length FROM {table} WHERE {column} IS NOT NULL AND (LENGTH(TRIM({column})) < 3 OR LENGTH(TRIM({column})) > 10)",
            "severity": "warning",
            "parameters": ["column"],
        },
        "postal_distribution": {
            "name": "Check Top Postal Code Distribution",
            "description": "Show top 20 most common postal codes",
            "command": "SELECT {column}, COUNT(*) AS cnt FROM {table} GROUP BY {column} ORDER BY cnt DESC LIMIT 20",
            "severity": "info",
            "parameters": ["column"],
        },
        "postal_all_zeros_check": {
            "name": "Check All-Zero Placeholder Codes",
            "description": "Find 00000 or 0000 placeholder codes",
            "command": "SELECT COUNT(*) AS zero_postal FROM {table} WHERE TRIM({column}) IN ('00000', '0000', '000', '00', '0')",
            "severity": "warning",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # COUNTRY COLUMNS
    # ══════════════════════════════════════════
    DataType.COUNTRY: {
        "country_null_check": {
            "name": "Check NULL Country Values",
            "description": "Count rows where country is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "country_empty_check": {
            "name": "Check Empty Country Strings",
            "description": "Find empty country values",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) = ''",
            "severity": "critical",
            "parameters": ["column"],
        },
        "country_iso2_length_check": {
            "name": "Check ISO-2 Code Length",
            "description": "Find country codes not exactly 2 characters (if using ISO-2 format)",
            "command": "SELECT COUNT(*) AS bad_iso2 FROM {table} WHERE {column} IS NOT NULL AND LENGTH(TRIM({column})) != 2",
            "severity": "warning",
            "parameters": ["column"],
        },
        "country_distribution": {
            "name": "Check Country Distribution",
            "description": "Show all country values with counts",
            "command": "SELECT {column}, COUNT(*) AS cnt FROM {table} GROUP BY {column} ORDER BY cnt DESC LIMIT 30",
            "severity": "info",
            "parameters": ["column"],
        },
        "country_lowercase_check": {
            "name": "Check Country Code Casing",
            "description": "Find lowercase country codes (ISO-2 should be uppercase)",
            "command": "SELECT COUNT(*) AS lowercase_count FROM {table} WHERE {column} IS NOT NULL AND {column} != UPPER({column})",
            "severity": "info",
            "parameters": ["column"],
        },
        "country_cardinality_check": {
            "name": "Check Country Cardinality",
            "description": "Count of distinct countries — more than 250 is suspicious",
            "command": "SELECT COUNT(DISTINCT {column}) AS country_count FROM {table}",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # CURRENCY / MONEY COLUMNS
    # ══════════════════════════════════════════
    DataType.CURRENCY: {
        "currency_null_check": {
            "name": "Check NULL Currency Values",
            "description": "Count rows where currency amount is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "currency_negative_check": {
            "name": "Check Negative Amounts",
            "description": "Find negative currency values (may be refunds or errors)",
            "command": "SELECT COUNT(*) AS negative_count FROM {table} WHERE {column} < 0",
            "severity": "warning",
            "parameters": ["column"],
        },
        "currency_zero_check": {
            "name": "Check Zero Amounts",
            "description": "Find zero currency values (may be missing or invalid)",
            "command": "SELECT COUNT(*) AS zero_count FROM {table} WHERE {column} = 0",
            "severity": "warning",
            "parameters": ["column"],
        },
        "currency_extreme_check": {
            "name": "Check Extreme Amounts",
            "description": "Find amounts above a very high threshold (>1,000,000)",
            "command": "SELECT COUNT(*) AS extreme_count FROM {table} WHERE {column} > 1000000",
            "severity": "warning",
            "parameters": ["column"],
        },
        "currency_range_check": {
            "name": "Check Currency Range Stats",
            "description": "Get min, max, average for the amount column",
            "command": "SELECT MIN({column}) AS min_amt, MAX({column}) AS max_amt, ROUND(AVG({column}), 2) AS avg_amt FROM {table} WHERE {column} IS NOT NULL",
            "severity": "info",
            "parameters": ["column"],
        },
        "currency_precision_check": {
            "name": "Check Excess Decimal Places",
            "description": "Find amounts with more than 2 decimal places",
            "command": "SELECT COUNT(*) AS excess_decimal FROM {table} WHERE {column} IS NOT NULL AND LENGTH(SUBSTR(CAST({column} AS TEXT), INSTR(CAST({column} AS TEXT), '.') + 1)) > 2 AND INSTR(CAST({column} AS TEXT), '.') > 0",
            "severity": "warning",
            "parameters": ["column"],
        },
        "currency_symbol_check": {
            "name": "Check Currency Symbols in Text",
            "description": "Find values with $ £ € ¥ symbols stored as text (should be numeric)",
            "command": "SELECT COUNT(*) AS symbol_count FROM {table} WHERE {column} LIKE '$%' OR {column} LIKE '£%' OR {column} LIKE '€%' OR {column} LIKE '¥%'",
            "severity": "critical",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # PERCENTAGE COLUMNS
    # ══════════════════════════════════════════
    DataType.PERCENTAGE: {
        "pct_null_check": {
            "name": "Check NULL Percentage Values",
            "description": "Count rows where percentage is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "pct_range_check": {
            "name": "Check Percentage Range (0-100)",
            "description": "Find percentages outside valid 0-100 range",
            "command": "SELECT COUNT(*) AS out_of_range FROM {table} WHERE {column} IS NOT NULL AND ({column} < 0 OR {column} > 100)",
            "severity": "critical",
            "parameters": ["column"],
        },
        "pct_decimal_range_check": {
            "name": "Check Decimal Percentage Range (0.0-1.0)",
            "description": "Find decimal percentages outside 0.0 to 1.0 (if stored as fraction)",
            "command": "SELECT COUNT(*) AS out_of_range FROM {table} WHERE {column} IS NOT NULL AND ({column} < 0 OR {column} > 1)",
            "severity": "warning",
            "parameters": ["column"],
        },
        "pct_stats_check": {
            "name": "Check Percentage Statistics",
            "description": "Get min, max, average percentage",
            "command": "SELECT MIN({column}) AS min_pct, MAX({column}) AS max_pct, ROUND(AVG({column}), 4) AS avg_pct FROM {table} WHERE {column} IS NOT NULL",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # AGE COLUMNS
    # ══════════════════════════════════════════
    DataType.AGE: {
        "age_null_check": {
            "name": "Check NULL Age Values",
            "description": "Count rows where age is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "age_negative_check": {
            "name": "Check Negative Ages",
            "description": "Find negative age values",
            "command": "SELECT COUNT(*) AS negative_age FROM {table} WHERE {column} < 0",
            "severity": "critical",
            "parameters": ["column"],
        },
        "age_impossible_check": {
            "name": "Check Impossible Ages (>130)",
            "description": "Find ages above 130 — biologically impossible",
            "command": "SELECT COUNT(*) AS impossible_age FROM {table} WHERE {column} > 130",
            "severity": "critical",
            "parameters": ["column"],
        },
        "age_range_check": {
            "name": "Check Age Range Statistics",
            "description": "Get min, max, average age",
            "command": "SELECT MIN({column}) AS min_age, MAX({column}) AS max_age, ROUND(AVG({column}), 1) AS avg_age FROM {table} WHERE {column} IS NOT NULL",
            "severity": "info",
            "parameters": ["column"],
        },
        "age_zero_check": {
            "name": "Check Zero Ages",
            "description": "Find records with age = 0 (may be infants or missing data)",
            "command": "SELECT COUNT(*) AS zero_age FROM {table} WHERE {column} = 0",
            "severity": "info",
            "parameters": ["column"],
        },
        "age_distribution_check": {
            "name": "Check Age Distribution",
            "description": "Show age distribution in 10-year buckets",
            "command": "SELECT (CAST({column} / 10 AS INTEGER) * 10) AS age_bucket, COUNT(*) AS cnt FROM {table} WHERE {column} IS NOT NULL GROUP BY age_bucket ORDER BY age_bucket",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # JSON / SEMI-STRUCTURED COLUMNS
    # ══════════════════════════════════════════
    DataType.JSON_COL: {
        "json_null_check": {
            "name": "Check NULL JSON Values",
            "description": "Count rows where JSON column is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "json_empty_check": {
            "name": "Check Empty JSON Values",
            "description": "Find rows with empty string or empty object/array",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) IN ('', '{}', '[]', 'null')",
            "severity": "warning",
            "parameters": ["column"],
        },
        "json_start_check": {
            "name": "Check JSON Start Character",
            "description": "Find values not starting with { or [ (invalid JSON)",
            "command": "SELECT COUNT(*) AS invalid_json FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) != '' AND SUBSTR(TRIM({column}), 1, 1) NOT IN ('{', '[')",
            "severity": "critical",
            "parameters": ["column"],
        },
        "json_length_check": {
            "name": "Check JSON Blob Size",
            "description": "Find suspiciously large JSON blobs (> 10,000 chars)",
            "command": "SELECT COUNT(*) AS large_json FROM {table} WHERE LENGTH({column}) > 10000",
            "severity": "info",
            "parameters": ["column"],
        },
        "json_sample_check": {
            "name": "Sample JSON Values",
            "description": "Show sample JSON values for structure inspection",
            "command": "SELECT SUBSTR({column}, 1, 200) AS json_preview FROM {table} WHERE {column} IS NOT NULL LIMIT 5",
            "severity": "info",
            "parameters": ["column"],
        },
    },

    # ══════════════════════════════════════════
    # TEXT (long-form free text) COLUMNS
    # ══════════════════════════════════════════
    DataType.TEXT: {
        "text_null_check": {
            "name": "Check NULL Text Values",
            "description": "Count rows where text column is NULL",
            "command": "SELECT COUNT(*) AS null_count FROM {table} WHERE {column} IS NULL",
            "severity": "warning",
            "parameters": ["column"],
        },
        "text_empty_check": {
            "name": "Check Empty Text Values",
            "description": "Find empty or whitespace-only text values",
            "command": "SELECT COUNT(*) AS empty_count FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) = ''",
            "severity": "warning",
            "parameters": ["column"],
        },
        "text_very_short_check": {
            "name": "Check Very Short Text",
            "description": "Find text values shorter than 3 characters (may be placeholders)",
            "command": "SELECT COUNT(*) AS too_short FROM {table} WHERE {column} IS NOT NULL AND LENGTH(TRIM({column})) < 3",
            "severity": "info",
            "parameters": ["column"],
        },
        "text_very_long_check": {
            "name": "Check Very Long Text",
            "description": "Find extremely long text values (> 5000 chars) that may indicate data issues",
            "command": "SELECT COUNT(*) AS too_long FROM {table} WHERE LENGTH({column}) > 5000",
            "severity": "info",
            "parameters": ["column"],
        },
        "text_length_stats": {
            "name": "Check Text Length Statistics",
            "description": "Get min, max, average text length",
            "command": "SELECT MIN(LENGTH({column})) AS min_len, MAX(LENGTH({column})) AS max_len, ROUND(AVG(LENGTH({column})), 0) AS avg_len FROM {table} WHERE {column} IS NOT NULL",
            "severity": "info",
            "parameters": ["column"],
        },
        "text_html_check": {
            "name": "Check HTML Tags in Text",
            "description": "Detect HTML tags stored in free-text fields (may be injection or unstripped data)",
            "command": "SELECT COUNT(*) AS html_count FROM {table} WHERE {column} LIKE '%<%>%' OR {column} LIKE '%</%>'",
            "severity": "warning",
            "parameters": ["column"],
        },
        "text_placeholder_check": {
            "name": "Check Placeholder Text",
            "description": "Detect common placeholder text: lorem, TODO, placeholder, TBD, test",
            "command": "SELECT COUNT(*) AS placeholder_count FROM {table} WHERE LOWER({column}) LIKE '%lorem ipsum%' OR LOWER({column}) LIKE '%todo%' OR LOWER({column}) LIKE '%placeholder%' OR LOWER({column}) LIKE '%tbd%'",
            "severity": "warning",
            "parameters": ["column"],
        },
    },
}


# ═══════════════════════════════════════════════════════════════
# TOOL EXECUTOR CLASS
# ═══════════════════════════════════════════════════════════════
class ValidationToolExecutor:
    """Executes pre-built validation tools and returns structured results."""

    def __init__(self, connector, table_name: str, selected_columns: Optional[List[str]] = None):
        self.connector = connector
        self.table_name = table_name
        self.selected_columns = selected_columns
        print(f"[DEBUG] ValidationToolExecutor init: table={table_name}, selected_columns={selected_columns}")

    async def execute_tool(self, tool_id: str, column: str = None, **kwargs) -> ToolResult:
        """Execute a single validation tool — checks UNIVERSAL, TABLE, and COLUMN tools."""
        import time
        start_time = time.time()

        # ── AUTH CHECK: Ensure column is in selected scope ────────────────────
        if self.selected_columns and column:
            if column not in self.selected_columns:
                print(f"[SECURITY] Blocked unauthorized access to column: {column}")
                return ToolResult(
                    tool_id=tool_id,
                    tool_name=tool_id,
                    command_executed="BLOCK_UNAUTHORIZED_SCOPE",
                    status="skipped",
                    row_count=0,
                    failed_count=0,
                    sample_rows=[],
                    message=f"Unauthorized column access: '{column}' is not in selected scope.",
                    severity="info",
                    column_name=column,
                )

        # ── Tool lookup: Universal → Table → Column ──────────────────
        tool_def = None
        tool_category = None

        if tool_id in UNIVERSAL_TOOLS:
            tool_def = UNIVERSAL_TOOLS[tool_id]
            tool_category = "universal"
        elif tool_id in TABLE_TOOLS:
            tool_def = TABLE_TOOLS[tool_id]
            tool_category = "table"
        else:
            for dtype, tools in COLUMN_TOOLS.items():
                if tool_id in tools:
                    tool_def = tools[tool_id]
                    tool_category = dtype.value
                    break

        if not tool_def:
            return ToolResult(
                tool_id=tool_id,
                tool_name="Unknown",
                command_executed="",
                status="error",
                row_count=0,
                failed_count=0,
                sample_rows=[],
                message=f"Tool '{tool_id}' not found in UNIVERSAL, TABLE, or COLUMN tools",
                severity="critical",
                column_name=column,
            )

        # ── Build command by substituting placeholders ────────────────
        command = tool_def["command"]
        
        # Helper to wrap identifiers in double quotes
        def q(name):
            if not name: return name
            safe_name = str(name).replace('"', '""')
            return f'"{safe_name}"'

        command = command.replace("{table}", q(self.table_name))
        if column:
            command = command.replace("{column}", q(column))

        if "{max_length}" in command:
            command = command.replace("{max_length}", str(kwargs.get("max_length", 255)))
        if "{max_decimals}" in command:
            command = command.replace("{max_decimals}", str(kwargs.get("max_decimals", 2)))
        if "{expected_values}" in command:
            values = kwargs.get("expected_values", [])
            command = command.replace("{expected_values}", ", ".join(f"'{v}'" for v in values))
        if "{deprecated_values}" in command:
            values = kwargs.get("deprecated_values", [])
            command = command.replace("{deprecated_values}", ", ".join(f"'{v}'" for v in values))
        if "{all_columns}" in command:
            # Use provided selected_columns if available, otherwise fetch from schema
            if self.selected_columns:
                all_cols = self.selected_columns
                print(f"[DEBUG] execute_tool: Using selected_columns for {{all_columns}}: {all_cols}")
            else:
                schema = await self.connector.get_schema(self.table_name)
                if isinstance(schema, dict):
                    all_cols = list(schema.get("columns", {}).keys())
                else:
                    all_cols = [c.get("name") for c in schema if isinstance(c, dict)]
                print(f"[DEBUG] execute_tool: No selected_columns, fetched {len(all_cols)} from schema")
            
            expanded = ", ".join(q(c) for c in all_cols)
            print(f"[DEBUG] execute_tool: Placeholder {{all_columns}} -> {expanded}")
            command = command.replace("{all_columns}", expanded)
        
        print(f"[DEBUG] execute_tool: Final SQL -> {command.strip()}")
        if "{columns}" in command:
            cols = kwargs.get("columns", [])
            command = command.replace("{columns}", ", ".join(q(c) for c in cols))

        # Skip if required parameters are still unresolved
        if "{expected_values}" in command or "{deprecated_values}" in command:
            return ToolResult(
                tool_id=tool_id,
                tool_name=tool_def["name"],
                command_executed=command.strip(),
                status="skipped",
                row_count=0,
                failed_count=0,
                sample_rows=[],
                message="Skipped: required parameter not provided",
                severity="info",
                column_name=column,
            )

        # ── Execute ───────────────────────────────────────────────────
        try:
            result = await self.connector.execute_raw_query(command)
            execution_time = int((time.time() - start_time) * 1000)

            if result.get("status") == "success":
                row_count = result.get("row_count", 0)
                sample_rows = result.get("sample_rows", [])[:5]

                # Smart failed_count: COUNT(*) returns 1 row with the count value
                actual_failed = row_count
                count_keywords = [
                    "count", "invalid", "empty", "long", "future", "old", "format",
                    "zero", "negative", "typo", "special", "deprecated", "length",
                    "null", "padded", "placeholder", "bad", "duplicate", "short",
                    "extreme", "gap", "epoch", "loopback", "insecure", "uppercase",
                    "private", "mixed", "nan", "nil", "excess", "symbol", "html",
                ]
                if row_count == 1 and sample_rows:
                    first_row = sample_rows[0]
                    for key, val in first_row.items():
                        if any(k in key.lower() for k in count_keywords):
                            try:
                                actual_failed = int(val) if val is not None else 0
                            except (ValueError, TypeError):
                                actual_failed = row_count
                            break

                return ToolResult(
                    tool_id=tool_id,
                    tool_name=tool_def["name"],
                    command_executed=command.strip(),
                    status="success",
                    row_count=row_count,
                    failed_count=actual_failed,
                    sample_rows=sample_rows,
                    message=f"Executed successfully. {actual_failed} issue(s) found.",
                    severity=tool_def.get("severity", "info"),
                    column_name=column,
                    execution_time_ms=execution_time,
                )
            else:
                return ToolResult(
                    tool_id=tool_id,
                    tool_name=tool_def["name"],
                    command_executed=command.strip(),
                    status="error",
                    row_count=0,
                    failed_count=0,
                    sample_rows=[],
                    message=result.get("error", "Unknown error"),
                    severity="critical",
                    column_name=column,
                    execution_time_ms=int((time.time() - start_time) * 1000),
                )
        except Exception as e:
            return ToolResult(
                tool_id=tool_id,
                tool_name=tool_def.get("name", tool_id),
                command_executed=command.strip() if "command" in locals() else "",
                status="error",
                row_count=0,
                failed_count=0,
                sample_rows=[],
                message=str(e),
                severity="critical",
                column_name=column,
                execution_time_ms=int((time.time() - start_time) * 1000),
            )

    async def execute_batch(self, tool_requests: List[Dict]) -> List[ToolResult]:
        """Execute a batch of tools."""
        results = []
        for request in tool_requests[:BATCH_SIZE]:
            tool_id = request.get("tool_id")
            column = request.get("column")
            kwargs = request.get("kwargs", {})
            results.append(await self.execute_tool(tool_id, column, **kwargs))
        return results

    def get_universal_tool_ids(self) -> List[str]:
        """Return list of universal tool IDs to be prepended to every column."""
        return list(UNIVERSAL_TOOLS.keys())

    def get_available_tools(self, column_name: str, column_type: str) -> List[Dict]:
        """
        Get tools for a column = UNIVERSAL tools + type-specific COLUMN tools.
        Universal null/empty/whitespace checks are always included first.
        """
        # ── Universal tools (always first) ───────────────────────────
        universal = [
            {
                "tool_id": tid,
                "name": tdef["name"],
                "description": tdef["description"],
                "severity": tdef.get("severity", "info"),
                "parameters": tdef.get("parameters", []),
                "category": "universal",
            }
            for tid, tdef in UNIVERSAL_TOOLS.items()
        ]

        # ── Type-specific tools ───────────────────────────────────────
        type_mapping = {
            "INTEGER": DataType.INTEGER, "INT": DataType.INTEGER,
            "BIGINT": DataType.INTEGER, "SMALLINT": DataType.INTEGER,
            "TINYINT": DataType.INTEGER,
            "TEXT": DataType.STRING, "VARCHAR": DataType.STRING,
            "CHAR": DataType.STRING, "STRING": DataType.STRING,
            "NVARCHAR": DataType.STRING, "CLOB": DataType.TEXT,
            "EMAIL": DataType.EMAIL, "PHONE": DataType.PHONE,
            "DATE": DataType.DATE,
            "DATETIME": DataType.DATETIME, "TIMESTAMP": DataType.DATETIME,
            "FLOAT": DataType.FLOAT, "DOUBLE": DataType.FLOAT,
            "DECIMAL": DataType.FLOAT, "NUMERIC": DataType.FLOAT,
            "REAL": DataType.FLOAT, "NUMBER": DataType.FLOAT,
            "BOOLEAN": DataType.BOOLEAN, "BOOL": DataType.BOOLEAN,
            "UUID": DataType.UUID, "GUID": DataType.UUID,
            "JSON": DataType.JSON_COL, "JSONB": DataType.JSON_COL,
        }

        detected_type = type_mapping.get(column_type.upper(), DataType.UNKNOWN)

        # Name-based semantic overrides
        name_lower = column_name.lower()
        if "email" in name_lower:
            detected_type = DataType.EMAIL
        elif any(k in name_lower for k in ("phone", "mobile", "cell", "tel")):
            detected_type = DataType.PHONE
        elif any(k in name_lower for k in ("uuid", "guid")):
            detected_type = DataType.UUID
        elif "url" in name_lower or "link" in name_lower or "website" in name_lower:
            detected_type = DataType.URL
        elif any(k in name_lower for k in ("ip_address", "ipaddress", "ip_addr")):
            detected_type = DataType.IP
        elif any(k in name_lower for k in ("zip", "postal", "postcode")):
            detected_type = DataType.POSTAL
        elif "country" in name_lower:
            detected_type = DataType.COUNTRY
        elif any(k in name_lower for k in ("price", "amount", "cost", "revenue", "fee", "salary", "balance")):
            if detected_type in (DataType.FLOAT, DataType.INTEGER, DataType.UNKNOWN):
                detected_type = DataType.CURRENCY
        elif any(k in name_lower for k in ("percent", "pct", "rate", "ratio")):
            detected_type = DataType.PERCENTAGE
        elif "age" in name_lower and detected_type in (DataType.INTEGER, DataType.UNKNOWN):
            detected_type = DataType.AGE
        elif any(k in name_lower for k in ("json", "metadata", "payload", "data")):
            detected_type = DataType.JSON_COL
        elif any(k in name_lower for k in ("description", "notes", "comment", "text", "body", "message", "bio")):
            detected_type = DataType.TEXT
        elif detected_type == DataType.UNKNOWN:
            if name_lower.endswith("_at") or name_lower in ("created_at", "updated_at", "deleted_at"):
                detected_type = DataType.DATETIME
            elif name_lower.endswith("_date") or name_lower == "date":
                detected_type = DataType.DATE
            elif any(k in name_lower for k in ("status", "state", "type", "category", "stage", "tier")):
                detected_type = DataType.CATEGORICAL
            elif any(k in name_lower for k in ("is_", "has_", "flag", "bool")):
                detected_type = DataType.BOOLEAN

        # Fetch type-specific tools
        type_tools = COLUMN_TOOLS.get(detected_type, COLUMN_TOOLS.get(DataType.STRING, {}))
        specific = [
            {
                "tool_id": tid,
                "name": tdef["name"],
                "description": tdef["description"],
                "severity": tdef.get("severity", "info"),
                "parameters": tdef.get("parameters", []),
                "category": detected_type.value,
            }
            for tid, tdef in type_tools.items()
        ]

        # Deduplicate: universal null/empty tools supersede type-specific duplicates
        universal_ids = {t["tool_id"] for t in universal}
        specific = [t for t in specific if t["tool_id"] not in universal_ids]

        return universal + specific

    def get_table_tools(self) -> List[Dict]:
        """Get list of all table-level tools."""
        return [
            {
                "tool_id": tool_id,
                "name": tool_def["name"],
                "description": tool_def["description"],
                "category": tool_def["category"],
                "priority": tool_def["priority"],
                "parameters": tool_def.get("parameters", []),
            }
            for tool_id, tool_def in TABLE_TOOLS.items()
        ]


# ═══════════════════════════════════════════════════════════════
# AGENT STATE (Extended for Tool-Based Agent)
# ═══════════════════════════════════════════════════════════════
from typing import TypedDict, Annotated
import operator

class ToolAgentState(TypedDict):
    """LangGraph agent state for tool-based validation."""
    validation_id: str
    validation_mode: ValidationMode
    data_source_info: DataSourceInfo
    custom_rules: List[Any]
    execution_config: Dict[str, Any]
    status: AgentStatus
    current_step: str
    messages: Annotated[List[Dict[str, Any]], operator.add]
    data_profile: Optional[DataProfile]
    ai_recommended_rules: List[Any]
    all_rules: List[Any]
    validation_results: List[Any]
    retrieved_context: List[Dict[str, Any]]
    exploration_steps: int
    validation_steps: int
    current_column_index: int
    columns_to_validate: List[Dict]
    available_column_tools: Dict[str, List[Dict]]
    quality_score: Optional[float]
    summary_report: Optional[Dict[str, Any]]
    error_message: Optional[str]
    started_at: Optional[str]
    completed_at: Optional[str]
    execution_metrics: Dict[str, Any]
    tool_execution_history: List[Dict]
    executed_queries: List[str]
    queries_per_column: Dict[str, int]