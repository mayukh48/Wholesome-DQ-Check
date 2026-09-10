"""Cross-dataset consistency check: compare a summed `column`, grouped by
`columns`, between this dataset and a reference dataset - e.g. sum(amount)
per sub_segment in a raw table vs. a downstream YTD/aggregate table.

A group "matches" when its relative difference is within `tolerance`
(default 0 = exact match); the check passes when the fraction of matching
groups meets `threshold`.
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
    if not check.columns:
        raise ValueError(
            f"check '{check.name}' (cross_dataset_consistency) needs 'columns' to group by"
        )
    if ref_df is None:
        raise ValueError(f"check '{check.name}' needs ref_dfs['{check.ref_dataset}'] to be supplied")

    tolerance = check.tolerance if check.tolerance is not None else 0.0
    left = df.groupBy(*check.columns).agg(F.sum(check.column).alias("_dq_left"))
    right = ref_df.groupBy(*check.columns).agg(F.sum(check.column).alias("_dq_right"))
    joined = left.join(right, on=check.columns, how="full_outer").fillna(
        0.0, subset=["_dq_left", "_dq_right"]
    )
    joined = joined.withColumn(
        "_dq_match",
        F.when(F.col("_dq_right") == 0, F.col("_dq_left") == 0).otherwise(
            (F.abs(F.col("_dq_left") - F.col("_dq_right")) / F.abs(F.col("_dq_right"))) <= tolerance
        ),
    )
    total_groups = joined.count()
    matched = joined.filter(F.col("_dq_match")).count()
    pass_rate = 1.0 if total_groups == 0 else matched / total_groups
    ok = pass_rate >= check.threshold
    message = f"{total_groups - matched}/{total_groups} groups mismatched beyond tolerance={tolerance}"
    return CheckOutcome(ok=ok, message=message, pass_rate=pass_rate)
