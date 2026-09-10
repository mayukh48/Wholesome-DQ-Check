"""Fuzzy-duplicate check: flags rows whose `column` is a near-match
(Levenshtein edit distance <= `max_edit_distance`, default 2) of another
row's - "Jon Smith" vs. "John Smith" - the same real-world entity recorded
under two different spellings, which `uniqueness` can't see because it
only catches an *exact* repeated value.

Comparing every row against every other row is O(n^2) and gets expensive
fast, so pass `columns` as a blocking key (e.g. `columns: [postal_code]` or
`[last_name]`) to only compare rows that already share something in
common - a normal, standard technique for fuzzy matching at scale. Without
`columns`, every row is compared against every other row; only use that on
a dataset small enough (or already scoped via `filter_expression`) for an
O(n^2) comparison to be reasonable.
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
    max_edit_distance = check.max_edit_distance if check.max_edit_distance is not None else 2
    block_cols = check.columns or []

    working = df.filter(F.col(check.column).isNotNull()).withColumn(
        "_dq_row_id", F.monotonically_increasing_id()
    )
    total = working.count()

    left = working.select(*block_cols, F.col("_dq_row_id").alias("_dq_id_a"), F.col(check.column).alias("_dq_val_a"))
    right = working.select(*block_cols, F.col("_dq_row_id").alias("_dq_id_b"), F.col(check.column).alias("_dq_val_b"))

    if block_cols:
        pairs = left.join(right, on=block_cols, how="inner")
    else:
        pairs = left.crossJoin(right)
    pairs = pairs.filter(F.col("_dq_id_a") < F.col("_dq_id_b"))

    near_dupes = pairs.filter(
        (F.col("_dq_val_a") != F.col("_dq_val_b"))  # an exact match is uniqueness's job, not this check's
        & (F.levenshtein(F.col("_dq_val_a"), F.col("_dq_val_b")) <= max_edit_distance)
    )

    flagged = (
        near_dupes.select(F.col("_dq_id_a").alias("_dq_id"))
        .union(near_dupes.select(F.col("_dq_id_b").alias("_dq_id")))
        .distinct()
        .count()
    )
    pass_rate = 1.0 if total == 0 else (total - flagged) / total
    ok = pass_rate >= check.threshold
    return CheckOutcome(
        ok=ok,
        message=f"near_duplicate_rows={flagged}/{total} (max_edit_distance={max_edit_distance})",
        pass_rate=pass_rate,
        total_rows=total,
        failed_rows=flagged,
    )
