"""Distribution-shift check: the proportion of each value in a categorical
`column` should stay close to the distribution recorded for this check in
the last run.

Stores today's per-category proportions as a JSON string in `metric_text`
so the next run has something to compare against - the same history
mechanism `anomaly`/`immutability` use, just for a whole distribution
instead of a single number. `max_pct_change` (reused from `anomaly`,
default 0.1 = 10 percentage points) bounds the largest allowed shift in any
one category's share. Passes automatically until there's a prior
distribution to compare against.
"""
from __future__ import annotations

import json
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

    counts = non_null.groupBy(check.column).count().collect()
    today_dist = {str(r[check.column]): r["count"] / total for r in counts}
    today_json = json.dumps(today_dist, sort_keys=True)

    if history_df is None:
        return CheckOutcome(
            ok=True, message="no history available yet - baseline not established", metric_text=today_json
        )

    prior_rows = (
        history_df.filter((F.col("dataset") == dataset_name) & (F.col("check_name") == check.name))
        .orderBy(F.col("run_timestamp").desc())
        .limit(1)
        .select("metric_text")
        .collect()
    )
    if not prior_rows or prior_rows[0]["metric_text"] is None:
        return CheckOutcome(ok=True, message="no prior distribution recorded yet", metric_text=today_json)

    try:
        baseline_dist = json.loads(prior_rows[0]["metric_text"])
    except (ValueError, TypeError):
        return CheckOutcome(
            ok=True,
            message="prior distribution could not be parsed - treating as no baseline",
            metric_text=today_json,
        )

    max_pct_change = check.max_pct_change if check.max_pct_change is not None else 0.1
    worst_category, max_diff = None, 0.0
    for cat in set(today_dist) | set(baseline_dist):
        diff = abs(today_dist.get(cat, 0.0) - baseline_dist.get(cat, 0.0))
        if diff > max_diff:
            worst_category, max_diff = cat, diff

    ok = max_diff <= max_pct_change
    pass_rate = max(0.0, 1.0 - max_diff)
    message = f"max proportion shift={max_diff:.3f} for category '{worst_category}' (limit {max_pct_change})"
    return CheckOutcome(ok=ok, message=message, pass_rate=pass_rate, metric_text=today_json)
