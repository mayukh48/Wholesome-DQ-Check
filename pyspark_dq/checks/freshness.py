"""Freshness check: the most recent timestamp in `column` isn't older than
`max_age_hours`."""
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
    if check.max_age_hours is None:
        raise ValueError(f"check '{check.name}' (freshness) needs 'max_age_hours'")

    max_ts = df.agg(F.max(F.col(check.column)).alias("max_ts")).collect()[0]["max_ts"]
    if max_ts is None:
        # Every value is null (or the dataset is empty) - there is no "latest"
        # timestamp to judge freshness against, so we treat that as a failure.
        return CheckOutcome(ok=False, message="no non-null timestamps found")

    # Compare the max timestamp to "now" using Spark's own clock (not the driver's
    # Python clock) so the result is consistent regardless of where this runs.
    age_hours = df.sparkSession.sql(
        "SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(TIMESTAMP %s)) / 3600.0 AS age_hours"
        % repr(str(max_ts))
    ).collect()[0]["age_hours"]

    ok = age_hours <= check.max_age_hours
    return CheckOutcome(ok=ok, message=f"latest_value={max_ts}, age_hours={age_hours:.2f}")
