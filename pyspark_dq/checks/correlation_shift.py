"""Correlation-shift check: the Pearson correlation between `column` and
`match_column` should stay close to the average correlation recorded for
this check over the last `lookback` (default 5) runs.

Uses `abs_tolerance` (default 0.2) rather than a percentage change, since
correlation is already bounded to [-1, 1] and can cross zero, where a
percentage change is meaningless (going from 0.01 to 0.5 is a 4900% swing
that's actually tiny in absolute terms). Passes automatically until
there's a baseline to compare against, same as `anomaly`.
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
    if not check.match_column:
        raise ValueError(
            f"check '{check.name}' (correlation_shift) needs 'match_column' - the other column to correlate against"
        )

    corr = df.stat.corr(check.column, check.match_column)
    if corr is None:
        return CheckOutcome(ok=True, message="correlation could not be computed (insufficient non-null data)")

    if history_df is None:
        return CheckOutcome(
            ok=True,
            message=f"correlation={corr:.3f} (no history available yet - baseline not established)",
            metric_value=corr,
        )

    lookback = check.lookback or 5
    baseline = (
        history_df.filter((F.col("dataset") == dataset_name) & (F.col("check_name") == check.name))
        .orderBy(F.col("run_timestamp").desc())
        .limit(lookback)
        .agg(F.avg("metric_value").alias("baseline"))
        .collect()[0]["baseline"]
    )
    if baseline is None:
        return CheckOutcome(
            ok=True, message=f"correlation={corr:.3f} (no prior runs recorded yet)", metric_value=corr
        )

    baseline = float(baseline)
    abs_tolerance = check.abs_tolerance if check.abs_tolerance is not None else 0.2
    diff = abs(corr - baseline)
    ok = diff <= abs_tolerance
    message = f"correlation={corr:.3f}, baseline={baseline:.3f} (avg of last {lookback} runs), diff={diff:.3f}"
    return CheckOutcome(ok=ok, message=message, metric_value=corr)
