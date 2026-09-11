"""Typed definitions for data quality checks, configs, and results."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

VALID_SEVERITIES = ("critical", "warning")

VALID_CHECK_TYPES = (
    "completeness",
    "uniqueness",
    "range",
    "value_set",
    "regex",
    "referential_integrity",
    "row_count",
    "freshness",
    "schema",
    "reconciliation",
    "cross_dataset_consistency",
    "anomaly",
    "expression",
    "accuracy",
    "immutability",
    "coverage",
    "period_gap",
    "sentinel_value",
    "stale_record",
    "derived_field",
    "outlier",
    "scd_overlap",
    "uniform_value",
    "castable",
    "length",
    "fuzzy_duplicate",
    "cross_source_duplicate",
    "monotonicity",
    "no_circular_reference",
    "format_consistency",
    "distribution_shift",
    "correlation_shift",
)


@dataclass
class CheckDefinition:
    name: str
    type: str
    severity: str = "warning"
    threshold: float = 1.0
    column: Optional[str] = None
    columns: Optional[List[str]] = None
    min: Optional[float] = None
    max: Optional[float] = None
    allowed_values: Optional[List[Any]] = None
    pattern: Optional[str] = None
    ref_dataset: Optional[str] = None
    ref_column: Optional[str] = None
    max_age_hours: Optional[float] = None
    expected_schema: Optional[Dict[str, str]] = None
    expression: Optional[str] = None
    tolerance: Optional[float] = None
    max_pct_change: Optional[float] = None
    lookback: Optional[int] = None
    match_column: Optional[str] = None
    ref_match_column: Optional[str] = None
    filter_expression: Optional[str] = None
    treat_blank_as_null: Optional[bool] = None
    disallowed_values: Optional[List[Any]] = None
    frequency: Optional[str] = None
    abs_tolerance: Optional[float] = None
    num_std_dev: Optional[float] = None
    start_column: Optional[str] = None
    end_column: Optional[str] = None
    value_map: Optional[Dict[Any, Any]] = None
    target_type: Optional[str] = None
    max_edit_distance: Optional[int] = None
    order_by: Optional[str] = None
    parent_column: Optional[str] = None
    max_depth: Optional[int] = None
    strict: Optional[bool] = None
    enforce_order: Optional[bool] = None
    format_patterns: Optional[List[str]] = None
    trim_whitespace: Optional[bool] = None
    metric: Optional[str] = None
    seasonal_period: Optional[str] = None

    def __post_init__(self) -> None:
        if self.type not in VALID_CHECK_TYPES:
            raise ValueError(f"Unknown check type '{self.type}' in check '{self.name}'")
        if self.severity not in VALID_SEVERITIES:
            raise ValueError(f"Invalid severity '{self.severity}' in check '{self.name}'")
        if not 0.0 <= self.threshold <= 1.0:
            raise ValueError(f"threshold must be between 0 and 1 in check '{self.name}'")
        if self.tolerance is not None and self.tolerance < 0:
            raise ValueError(f"tolerance must be >= 0 in check '{self.name}'")
        if self.max_pct_change is not None and self.max_pct_change < 0:
            raise ValueError(f"max_pct_change must be >= 0 in check '{self.name}'")
        if self.lookback is not None and self.lookback < 1:
            raise ValueError(f"lookback must be >= 1 in check '{self.name}'")
        if self.abs_tolerance is not None and self.abs_tolerance < 0:
            raise ValueError(f"abs_tolerance must be >= 0 in check '{self.name}'")
        if self.num_std_dev is not None and self.num_std_dev <= 0:
            raise ValueError(f"num_std_dev must be > 0 in check '{self.name}'")
        if self.max_edit_distance is not None and self.max_edit_distance < 0:
            raise ValueError(f"max_edit_distance must be >= 0 in check '{self.name}'")
        if self.max_depth is not None and self.max_depth < 1:
            raise ValueError(f"max_depth must be >= 1 in check '{self.name}'")


@dataclass
class DatasetConfig:
    dataset: str
    layer: str
    checks: List[CheckDefinition] = field(default_factory=list)


@dataclass
class CheckResult:
    dataset: str
    layer: str
    check_name: str
    check_type: str
    severity: str
    column: Optional[str]
    status: str  # "PASS" or "FAIL"
    threshold: float
    pass_rate: Optional[float]
    total_rows: Optional[int]
    failed_rows: Optional[int]
    message: str
    metric_value: Optional[float] = None
    metric_text: Optional[str] = None


@dataclass
class CheckOutcome:
    """What a single check module hands back to the engine: enough to build
    a CheckResult, without the module needing to know about dataset/layer/
    run-level bookkeeping (that's the engine's job, not the check's)."""

    ok: bool
    message: str
    pass_rate: Optional[float] = None
    total_rows: Optional[int] = None
    failed_rows: Optional[int] = None
    metric_value: Optional[float] = None
    metric_text: Optional[str] = None


class DQCriticalFailure(Exception):
    """Raised when one or more critical-severity checks fail."""

    def __init__(self, failures: List[CheckResult]):
        self.failures = failures
        summary = "; ".join(f"{f.check_name} ({f.dataset}.{f.column})" for f in failures)
        super().__init__(f"{len(failures)} critical data quality check(s) failed: {summary}")
