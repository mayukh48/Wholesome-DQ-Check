"""Anomaly check: flag a swing in a tracked metric against the average of
the last `lookback` (default 5) recorded runs for this same check, read
back from the DQ results table via `history_df`. Passes automatically -
with no baseline yet - until enough history has accumulated, so a fresh
dataset never false-fails on day one.

`metric` selects what's tracked: "sum" (sum of `column`, the default when
`column` is set), "count" (row count, the default when `column` is
omitted), "null_rate" (fraction of `column` that's null - catches a null
rate jumping from 1% to 40%), or "distinct_count" (number of distinct
values in `column` - catches a categorical column's cardinality changing
abruptly).

Set `seasonal_period` to compare against the same calendar period in
history instead of just the most recent runs - "month" (same month, prior
years), "day_of_week" (same weekday), or "day_of_month" (same date each
month, e.g. a month-end close). Without it, the baseline is the average of
the last `lookback` runs regardless of when they fell, which can miss or
dilute "expected seasonal trend doesn't occur" (a holiday spike that should
have happened, didn't).
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition, CheckOutcome

_SEASONAL_PERIOD_FNS = {
    "month": F.month,
    "day_of_week": F.dayofweek,
    "day_of_month": F.dayofmonth,
}


def _compute_metric(check: CheckDefinition, df: DataFrame) -> float:
    metric_kind = check.metric or ("sum" if check.column else "count")

    if metric_kind == "count":
        return float(df.count())

    if metric_kind not in ("sum", "null_rate", "distinct_count"):
        raise ValueError(
            f"check '{check.name}' (anomaly): unknown metric '{metric_kind}' "
            "(expected sum, count, null_rate, or distinct_count)"
        )
    if not check.column:
        raise ValueError(f"check '{check.name}' (anomaly): metric '{metric_kind}' needs 'column'")

    if metric_kind == "sum":
        return df.agg(F.sum(F.col(check.column))).collect()[0][0] or 0.0
    if metric_kind == "distinct_count":
        return float(df.select(check.column).distinct().count())

    # null_rate
    total = df.count()
    if total == 0:
        return 0.0
    nulls = df.filter(F.col(check.column).isNull()).count()
    return nulls / total


def evaluate(
    check: CheckDefinition,
    df: DataFrame,
    *,
    ref_df: Optional[DataFrame] = None,
    history_df: Optional[DataFrame] = None,
    dataset_name: Optional[str] = None,
) -> CheckOutcome:
    metric = _compute_metric(check, df)

    if history_df is None:
        return CheckOutcome(
            ok=True,
            message=f"metric={metric:g} (no history available yet - baseline not established)",
            metric_value=metric,
        )

    lookback = check.lookback or 5
    scoped_history = history_df.filter(
        (F.col("dataset") == dataset_name) & (F.col("check_name") == check.name)
    )

    if check.seasonal_period:
        if check.seasonal_period not in _SEASONAL_PERIOD_FNS:
            raise ValueError(
                f"check '{check.name}' (anomaly): seasonal_period must be one of "
                f"{sorted(_SEASONAL_PERIOD_FNS)}, got '{check.seasonal_period}'"
            )
        period_fn = _SEASONAL_PERIOD_FNS[check.seasonal_period]
        now_value = (
            df.sparkSession.range(1)
            .select(period_fn(F.current_date()).alias("p"))
            .collect()[0]["p"]
        )
        scoped_history = scoped_history.filter(period_fn(F.col("run_timestamp")) == now_value)

    baseline = (
        scoped_history.orderBy(F.col("run_timestamp").desc())
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

    period_note = "matching-period " if check.seasonal_period else ""
    message = (
        f"metric={metric:g}, baseline={baseline:.2f} (avg of last {lookback} {period_note}runs), "
        f"max_pct_change={max_pct_change}"
    )
    return CheckOutcome(ok=ok, message=message, metric_value=metric)
