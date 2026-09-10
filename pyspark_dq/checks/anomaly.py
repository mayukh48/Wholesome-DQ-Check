"""Anomaly check: flag a volume swing in a summed `column` (or row count, if
`column` is omitted) against the average of the last `lookback` (default 5)
recorded runs for this same check, read back from the DQ results table via
`history_df`. Passes automatically - with no baseline yet - until enough
history has accumulated, so a fresh dataset never false-fails on day one.
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
    if check.column:
        metric = df.agg(F.sum(F.col(check.column))).collect()[0][0] or 0.0
    else:
        metric = float(df.count())

    if history_df is None:
        return CheckOutcome(
            ok=True,
            message=f"metric={metric:g} (no history available yet - baseline not established)",
            metric_value=metric,
        )

    lookback = check.lookback or 5
    baseline = (
        history_df.filter(
            (F.col("dataset") == dataset_name) & (F.col("check_name") == check.name)
        )
        .orderBy(F.col("run_timestamp").desc())
        .limit(lookback)
        .agg(F.avg("metric_value").alias("baseline"))
        .collect()[0]["baseline"]
    )
    if baseline is None:
        return CheckOutcome(
            ok=True,
            message=f"metric={metric:g} (no prior runs recorded for this check yet)",
            metric_value=metric,
        )

    baseline = float(baseline)
    max_pct_change = check.max_pct_change if check.max_pct_change is not None else 0.5
    if baseline == 0:
        ok = metric == 0
    else:
        ok = abs(metric - baseline) / abs(baseline) <= max_pct_change

    message = (
        f"metric={metric:g}, baseline={baseline:.2f} (avg of last {lookback} runs), "
        f"max_pct_change={max_pct_change}"
    )
    return CheckOutcome(ok=ok, message=message, metric_value=metric)
