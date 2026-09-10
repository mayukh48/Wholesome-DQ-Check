"""Immutability check: detect that already-finalized (historical) data was
silently changed since a previous run, by fingerprinting `df` and comparing
against the fingerprint recorded for this check in the last run.

Scope this to the historical slice only, via `filter_expression` on the
check config (e.g. "report_date < date_trunc('month', current_date())") -
current-period rows are expected to change and shouldn't trip this check.
`columns` picks which columns to fingerprint (default: all of them).

Passes automatically until there's a prior fingerprint to compare against,
so a brand-new dataset/check never false-fails on day one - same pattern as
`anomaly`, but for "did anything change" rather than "did the metric swing."
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
    cols = check.columns or df.columns
    row = df.select(F.xxhash64(*[F.col(c) for c in cols]).alias("_dq_row_hash")).agg(
        F.sum("_dq_row_hash").alias("checksum")
    ).collect()[0]
    checksum = str(row["checksum"] if row["checksum"] is not None else 0)

    if history_df is None:
        return CheckOutcome(
            ok=True,
            message=f"checksum={checksum} (no history available yet - baseline not established)",
            metric_text=checksum,
        )

    prior_rows = (
        history_df.filter(
            (F.col("dataset") == dataset_name) & (F.col("check_name") == check.name)
        )
        .orderBy(F.col("run_timestamp").desc())
        .limit(1)
        .select("metric_text")
        .collect()
    )
    if not prior_rows or prior_rows[0]["metric_text"] is None:
        return CheckOutcome(
            ok=True,
            message=f"checksum={checksum} (no prior checksum recorded yet)",
            metric_text=checksum,
        )

    prior_checksum = prior_rows[0]["metric_text"]
    ok = checksum == prior_checksum
    message = (
        f"checksum unchanged ({checksum})"
        if ok
        else f"historical data changed: checksum={checksum} != prior_checksum={prior_checksum}"
    )
    return CheckOutcome(ok=ok, message=message, metric_text=checksum)
