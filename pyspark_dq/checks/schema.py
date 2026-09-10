"""Schema check: the DataFrame's actual dtypes must match `expected_schema`.

This is metadata-only (df.dtypes doesn't trigger a Spark job), so schema
checks are essentially free compared to the row-scanning checks. Because
Spark's dtype strings are fully recursive (a struct/array column's dtype
spells out its entire nested shape, e.g.
``struct<street:string,zipcode:int>``), this already catches nested schema
drift too, not just flat top-level columns.

By default, only the columns listed in `expected_schema` are checked (an
unlisted column existing is not itself a failure) and column order is
ignored. Set `strict: true` to also fail if the DataFrame has any column
*not* listed in `expected_schema` - catches a new, unmapped column quietly
showing up in the feed. Set `enforce_order: true` to additionally require
the expected columns to appear in the DataFrame in the same relative order
they're declared in `expected_schema` - catches a pipeline that (incorrectly)
assumed a fixed column position when the source reordered its columns.
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

    if check.strict:
        extra = [c for c in df.columns if c not in check.expected_schema]
        if extra:
            mismatches.append(f"unexpected column(s): {', '.join(extra)}")

    if check.enforce_order:
        expected_order = [c for c in check.expected_schema if c in actual]
        actual_order = [c for c in df.columns if c in check.expected_schema]
        if expected_order != actual_order:
            mismatches.append(f"column order mismatch: expected {expected_order}, got {actual_order}")

    ok = not mismatches
    message = "schema matches" if ok else "; ".join(mismatches)
    return CheckOutcome(ok=ok, message=message)
