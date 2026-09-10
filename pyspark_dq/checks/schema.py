"""Schema check: the DataFrame's actual dtypes must match `expected_schema`.

This is metadata-only (df.dtypes doesn't trigger a Spark job), so schema
checks are essentially free compared to the row-scanning checks.
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame

from ..models import CheckDefinition, CheckOutcome


def evaluate(
    check: CheckDefinition,
    df: DataFrame,
    *,
    ref_df: Optional[DataFrame] = None,
    history_df: Optional[DataFrame] = None,
    dataset_name: Optional[str] = None,
) -> CheckOutcome:
    if not check.expected_schema:
        raise ValueError(f"check '{check.name}' (schema) needs 'expected_schema'")
    actual = dict(df.dtypes)
    mismatches = []
    for col_name, expected_type in check.expected_schema.items():
        actual_type = actual.get(col_name)
        if actual_type is None:
            mismatches.append(f"missing column '{col_name}'")
        elif actual_type != expected_type:
            mismatches.append(f"'{col_name}' expected {expected_type}, got {actual_type}")
    ok = not mismatches
    message = "schema matches" if ok else "; ".join(mismatches)
    return CheckOutcome(ok=ok, message=message)
