"""SCD-overlap check: no two rows for the same entity (`columns`) may have
overlapping `[start_column, end_column)` validity windows.

Catches the classic SCD Type 2 defect: two "versions" of the same entity
both claim to be valid at the same point in time - a bad merge produced two
concurrently-active rows, or a correction created a version whose date
range overlaps the one it was meant to close out. A NULL `end_column` is
treated as "still open" (valid through the far future), the normal SCD
Type 2 convention for the current version of a record.

This can't be a plain `uniqueness` check: uniqueness only catches two rows
sharing the exact same key, not two rows whose *date ranges* overlap
without sharing a single identical value.
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition, CheckOutcome

_FAR_FUTURE = "9999-12-31"


def evaluate(
    check: CheckDefinition,
    df: DataFrame,
    *,
    ref_df: Optional[DataFrame] = None,
    history_df: Optional[DataFrame] = None,
    dataset_name: Optional[str] = None,
) -> CheckOutcome:
    if not check.columns:
        raise ValueError(f"check '{check.name}' (scd_overlap) needs 'columns' - the entity key")
    if not check.start_column or not check.end_column:
        raise ValueError(f"check '{check.name}' (scd_overlap) needs 'start_column' and 'end_column'")

    key_cols = check.columns
    working = (
        df.withColumn("_dq_row_id", F.monotonically_increasing_id())
        .withColumn("_dq_start", F.col(check.start_column).cast("timestamp"))
        .withColumn(
            "_dq_end",
            F.coalesce(F.col(check.end_column).cast("timestamp"), F.lit(_FAR_FUTURE).cast("timestamp")),
        )
    )

    left = working.select(
        *key_cols,
        F.col("_dq_row_id").alias("_dq_id_a"),
        F.col("_dq_start").alias("_dq_start_a"),
        F.col("_dq_end").alias("_dq_end_a"),
    )
    right = working.select(
        *key_cols,
        F.col("_dq_row_id").alias("_dq_id_b"),
        F.col("_dq_start").alias("_dq_start_b"),
        F.col("_dq_end").alias("_dq_end_b"),
    )

    # Self-join on the entity key; _dq_id_a < _dq_id_b keeps each pair of
    # versions exactly once (and drops a row pairing with itself).
    pairs = left.join(right, on=key_cols, how="inner").filter(F.col("_dq_id_a") < F.col("_dq_id_b"))
    overlaps = pairs.filter(
        (F.col("_dq_start_a") < F.col("_dq_end_b")) & (F.col("_dq_start_b") < F.col("_dq_end_a"))
    )

    total_entities = working.select(*key_cols).distinct().count()
    overlapping_entities = overlaps.select(*key_cols).distinct().count()
    pass_rate = 1.0 if total_entities == 0 else (total_entities - overlapping_entities) / total_entities
    ok = pass_rate >= check.threshold
    return CheckOutcome(
        ok=ok,
        message=f"entities_with_overlapping_versions={overlapping_entities}/{total_entities}",
        pass_rate=pass_rate,
        total_rows=total_entities,
        failed_rows=overlapping_entities,
    )
