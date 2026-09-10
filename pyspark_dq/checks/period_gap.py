"""Period-gap check: detect a missing period (day/month/year) inside the
observed range of a date/timestamp `column` - a gap in an otherwise
continuous time series, e.g. one missing month in a five-year trend.

This is a different failure mode from `freshness` (which only cares about
the *most recent* value) and from `row_count` (which can't see a hole in
the middle of an otherwise-populated table): it generates the full expected
sequence of periods between the observed min and max, and flags any period
with zero rows.

`frequency` is one of "day", "month", "year" (default "day").
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition, CheckOutcome
from ._util import require

_VALID_FREQUENCIES = ("day", "month", "year")


def evaluate(
    check: CheckDefinition,
    df: DataFrame,
    *,
    ref_df: Optional[DataFrame] = None,
    history_df: Optional[DataFrame] = None,
    dataset_name: Optional[str] = None,
) -> CheckOutcome:
    require(check, "column")
    frequency = check.frequency or "day"
    if frequency not in _VALID_FREQUENCIES:
        raise ValueError(
            f"check '{check.name}' (period_gap): frequency must be one of {_VALID_FREQUENCIES}, got '{frequency}'"
        )

    bounds = df.agg(
        F.min(F.col(check.column)).alias("min_dt"), F.max(F.col(check.column)).alias("max_dt")
    ).collect()[0]
    if bounds["min_dt"] is None or bounds["max_dt"] is None:
        return CheckOutcome(ok=False, message="no non-null dates found")

    spark = df.sparkSession
    expected = spark.range(1).select(
        F.explode(
            F.sequence(
                F.date_trunc(frequency, F.lit(bounds["min_dt"])),
                F.date_trunc(frequency, F.lit(bounds["max_dt"])),
                F.expr(f"INTERVAL 1 {frequency.upper()}"),
            )
        ).alias("period")
    )
    actual = df.select(F.date_trunc(frequency, F.col(check.column)).alias("period")).distinct()
    missing = expected.join(actual, on="period", how="left_anti")

    total_periods = expected.count()
    missing_count = missing.count()
    pass_rate = 1.0 if total_periods == 0 else (total_periods - missing_count) / total_periods
    ok = pass_rate >= check.threshold

    sample = [str(r["period"]) for r in missing.orderBy("period").limit(5).collect()]
    message = f"missing_periods={missing_count}/{total_periods}"
    if sample:
        message += f" (e.g. {', '.join(sample)})"

    return CheckOutcome(
        ok=ok,
        message=message,
        pass_rate=pass_rate,
        total_rows=total_periods,
        failed_rows=missing_count,
    )
