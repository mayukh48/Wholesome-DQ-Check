"""Cross-source duplicate check: flags keys in `column` that also appear in
a reference dataset's `ref_column` - records about to collide if the two
were merged/unioned as-is.

Two distinct real scenarios both reduce to the same question ("do these two
key-sets overlap"):

* **Merging two source systems** - `ref_dataset` is the other source about
  to be unioned in. An overlapping key means the same real-world record was
  ingested from both systems and would become a duplicate post-merge.
* **Backfill / late-arriving data** - `ref_dataset` is the target table
  this batch is about to be written into. An overlapping key means the
  backfill is about to re-insert a record that's already there.

`uniqueness` can't catch either case: it only checks for duplicates
*within* one already-merged dataset, not whether two *separate* datasets
are about to collide.
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
    if not check.ref_column:
        raise ValueError(f"check '{check.name}' (cross_source_duplicate) needs 'ref_column'")
    if ref_df is None:
        raise ValueError(f"check '{check.name}' needs ref_dfs['{check.ref_dataset}'] to be supplied")

    left_keys = (
        df.select(F.col(check.column).alias("_dq_key")).filter(F.col("_dq_key").isNotNull()).distinct()
    )
    right_keys = (
        ref_df.select(F.col(check.ref_column).alias("_dq_key"))
        .filter(F.col("_dq_key").isNotNull())
        .distinct()
    )
    overlap = left_keys.join(right_keys, on="_dq_key", how="inner")

    total = left_keys.count()
    overlap_count = overlap.count()
    pass_rate = 1.0 if total == 0 else (total - overlap_count) / total
    ok = pass_rate >= check.threshold
    return CheckOutcome(
        ok=ok,
        message=f"keys_present_in_both_sources={overlap_count}/{total}",
        pass_rate=pass_rate,
        total_rows=total,
        failed_rows=overlap_count,
    )
