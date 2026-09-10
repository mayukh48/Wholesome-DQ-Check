"""Row-count check: the dataset's row count must fall within [min, max].

Dataset-level: exposes evaluate(check, df, **kwargs) -> CheckOutcome. Every
dataset-level module gets the same keyword args (ref_df, history_df,
dataset_name) whether it needs them or not, so the engine never needs to
know which check types need what - it just routes and calls.
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
    total = df.count()
    ok = True
    if check.min is not None and total < check.min:
        ok = False
    if check.max is not None and total > check.max:
        ok = False
    return CheckOutcome(ok=ok, message=f"row_count={total}", total_rows=total)
