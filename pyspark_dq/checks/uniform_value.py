"""Uniform-value check: `column`'s non-null values must all be the same -
no silently mixed units/currencies/codes within one dataset.

Unlike `value_set`, this doesn't need a pre-known allow-list: pass a single
`allowed_values` entry to pin the expected value explicitly (e.g. "every row
in this file must be USD"), or omit it to just detect a split - the
majority value becomes the baseline and anything else counts as a mismatch.
Catches "same metric reported in different units within the same dataset"
- a currency column quietly mixing USD and EUR rows, or a weight column
mixing kg and lbs.
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition, CheckOutcome
from ._util import require


def evaluate(
    check: CheckDefinition,
    df: DataFrame,
    *,
    ref_df: Optional[DataFrame] = None,
    history_df: Optional[DataFrame] = None,
    dataset_name: Optional[str] = None,
) -> CheckOutcome:
    require(check, "column")
    non_null = df.filter(F.col(check.column).isNotNull())
    total = non_null.count()
    if total == 0:
        return CheckOutcome(ok=True, message="no non-null values present", total_rows=0, failed_rows=0)

    if check.allowed_values:
        expected = check.allowed_values[0]
    else:
        # No expected value given - use whichever value is most common as
        # the baseline, so this still detects a split without needing to
        # know in advance which value is "correct."
        top = non_null.groupBy(check.column).count().orderBy(F.desc("count")).first()
        expected = top[check.column]

    mismatched = non_null.filter(F.col(check.column) != F.lit(expected)).count()
    pass_rate = (total - mismatched) / total
    ok = pass_rate >= check.threshold
    distinct_count = non_null.select(check.column).distinct().count()
    message = f"{distinct_count} distinct value(s) found; {mismatched}/{total} rows disagree with '{expected}'"
    return CheckOutcome(ok=ok, message=message, pass_rate=pass_rate, total_rows=total, failed_rows=mismatched)
