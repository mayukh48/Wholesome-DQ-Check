"""Check implementations, one module per check type.

Import the whole package (``from pyspark_dq import checks``) to get the
registries the engine dispatches through, or import a single check's module
directly if that's all you need, e.g.::

    from pyspark_dq.checks import completeness
    df, predicate = completeness.build_predicate(check, df)

Row-level modules (batched by the engine into one aggregation per dataset)
expose ``build_predicate(check, df) -> (df, Column)``. Dataset-level modules
(each evaluated independently) expose
``evaluate(check, df, *, ref_df=None, history_df=None, dataset_name=None) ->
CheckOutcome``. Adding a new check type is: write a module with the right
shape, add one line below - the engine never needs to change.
"""
from . import (
    accuracy,
    anomaly,
    castable,
    completeness,
    correlation_shift,
    coverage,
    cross_dataset_consistency,
    cross_source_duplicate,
    derived_field,
    distribution_shift,
    expression,
    format_consistency,
    freshness,
    fuzzy_duplicate,
    immutability,
    length,
    monotonicity,
    no_circular_reference,
    outlier,
    period_gap,
    pii_exposure,
    range as range_check,
    reconciliation,
    referential_integrity,
    regex as regex_check,
    row_count,
    scd_overlap,
    schema as schema_check,
    sentinel_value,
    stale_record,
    uniform_value,
    uniqueness,
    value_set,
)

# check.type -> module exposing build_predicate(check, df) -> (df, Column).
ROW_LEVEL_CHECKS = {
    "completeness": completeness,
    "uniqueness": uniqueness,
    "range": range_check,
    "value_set": value_set,
    "regex": regex_check,
    "expression": expression,
    "sentinel_value": sentinel_value,
    "stale_record": stale_record,
    "derived_field": derived_field,
    "outlier": outlier,
    "castable": castable,
    "length": length,
    "monotonicity": monotonicity,
    "pii_exposure": pii_exposure,
}

# check.type -> module exposing evaluate(check, df, **kwargs) -> CheckOutcome.
DATASET_LEVEL_CHECKS = {
    "row_count": row_count,
    "freshness": freshness,
    "schema": schema_check,
    "referential_integrity": referential_integrity,
    "accuracy": accuracy,
    "reconciliation": reconciliation,
    "cross_dataset_consistency": cross_dataset_consistency,
    "anomaly": anomaly,
    "immutability": immutability,
    "coverage": coverage,
    "period_gap": period_gap,
    "scd_overlap": scd_overlap,
    "uniform_value": uniform_value,
    "fuzzy_duplicate": fuzzy_duplicate,
    "cross_source_duplicate": cross_source_duplicate,
    "no_circular_reference": no_circular_reference,
    "format_consistency": format_consistency,
    "distribution_shift": distribution_shift,
    "correlation_shift": correlation_shift,
}

ROW_LEVEL_TYPES = set(ROW_LEVEL_CHECKS)
DATASET_LEVEL_TYPES = set(DATASET_LEVEL_CHECKS)

__all__ = [
    "ROW_LEVEL_CHECKS",
    "DATASET_LEVEL_CHECKS",
    "ROW_LEVEL_TYPES",
    "DATASET_LEVEL_TYPES",
    "accuracy",
    "anomaly",
    "castable",
    "completeness",
    "correlation_shift",
    "coverage",
    "cross_dataset_consistency",
    "cross_source_duplicate",
    "derived_field",
    "distribution_shift",
    "expression",
    "format_consistency",
    "freshness",
    "fuzzy_duplicate",
    "immutability",
    "length",
    "monotonicity",
    "no_circular_reference",
    "outlier",
    "period_gap",
    "pii_exposure",
    "reconciliation",
    "referential_integrity",
    "row_count",
    "scd_overlap",
    "sentinel_value",
    "stale_record",
    "uniform_value",
    "uniqueness",
    "value_set",
]
