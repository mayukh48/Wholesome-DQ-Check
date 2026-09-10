"""Uniqueness check: `column` (or `columns`, for a composite key) must have
no duplicate values.

Row-level: exposes build_predicate(check, df) -> (df, boolean Column). The
df is "augmented" with a window-count column before the predicate can be
derived, so the row-level pass/fail flag can be reused directly in the
engine's shared batched aggregation.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..models import CheckDefinition


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    cols = check.columns or ([check.column] if check.column else None)
    if not cols:
        raise ValueError(f"check '{check.name}' (uniqueness) needs 'column' or 'columns'")

    dup_count_col = f"_dq_dupcount_{'_'.join(cols)}"
    if dup_count_col not in df.columns:
        w = Window.partitionBy(*cols)
        df = df.withColumn(dup_count_col, F.count(F.lit(1)).over(w))

    # Rows with a null key are excluded from the uniqueness verdict (that's
    # what the completeness check is for) so they're always treated as passing.
    any_null = F.lit(False)
    for c in cols:
        any_null = any_null | F.col(c).isNull()
    return df, (any_null | (F.col(dup_count_col) == 1))
