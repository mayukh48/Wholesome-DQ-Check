"""Reconciliation check: compare a row count (or a summed `column`) between
this dataset and a reference dataset - e.g. raw vs. processed counts after a
load, or this run's count against a prior/expected count. Also usable as an
idempotency guard by pointing `ref_dataset` at an "already loaded batches"
marker table.

`threshold` is the minimum required match ratio (1.0 = must match exactly).
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition, CheckOutcome


def evaluate(
    check: CheckDefinition,
    df: DataFrame,
    *,
    ref_df: Optional[DataFrame] = None,
    history_df: Optional[DataFrame] = None,
    dataset_name: Optional[str] = None,
) -> CheckOutcome:
    if ref_df is None:
        raise ValueError(f"check '{check.name}' needs ref_dfs['{check.ref_dataset}'] to be supplied")

    if check.column:
        actual = df.agg(F.sum(F.col(check.column))).collect()[0][0] or 0.0
        expected = ref_df.agg(F.sum(F.col(check.column))).collect()[0][0] or 0.0
    else:
        actual = float(df.count())
        expected = float(ref_df.count())

    if expected == 0:
        pass_rate = 1.0 if actual == 0 else 0.0
    else:
        pass_rate = max(0.0, 1.0 - abs(actual - expected) / abs(expected))

    ok = pass_rate >= check.threshold
    message = f"actual={actual:g}, expected={expected:g}, pass_rate={pass_rate:.4f}"
    return CheckOutcome(ok=ok, message=message, pass_rate=pass_rate, metric_value=actual)
