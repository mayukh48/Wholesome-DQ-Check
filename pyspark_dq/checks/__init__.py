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
    completeness,
    cross_dataset_consistency,
    expression,
    freshness,
    immutability,
    range as range_check,
    reconciliation,
    referential_integrity,
    regex as regex_check,
    row_count,
    schema as schema_check,
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
    "completeness",
    "cross_dataset_consistency",
    "expression",
    "freshness",
    "immutability",
    "reconciliation",
    "referential_integrity",
    "row_count",
    "uniqueness",
    "value_set",
]
