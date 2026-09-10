"""Monotonicity check: `column` must never decrease when rows are ordered
by `order_by` - flags out-of-order events in the arrival/processing
sequence, e.g. an event whose `event_timestamp` is earlier than the
previous row's despite arriving later in the stream.

Pass `columns` to check monotonicity separately within each partition (e.g.
per `device_id` or `stream_id`) instead of across the whole dataset.

Uses a window function (LAG) to compare each row against the previous one
in `order_by` order - the same window-augmentation pattern `uniqueness`
uses. Ordering the whole dataset without a partition key (no `columns`)
forces a single-partition sort, which doesn't scale to a huge table; scope
with `columns` and/or `filter_expression` on anything large.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    if not check.order_by:
        raise ValueError(
            f"check '{check.name}' (monotonicity) needs 'order_by' - the column defining arrival order"
        )

    partition_cols = check.columns or []
    w = Window.orderBy(check.order_by)
    if partition_cols:
        w = Window.partitionBy(*partition_cols).orderBy(check.order_by)

    prev_col = f"_dq_prev_{check.column}"
    if prev_col not in df.columns:
        df = df.withColumn(prev_col, F.lag(F.col(check.column)).over(w))

    # The first row in each partition has no predecessor to compare against
    # and always passes; a null `column` value is completeness's job.
    return df, (
        F.col(prev_col).isNull() | F.col(check.column).isNull() | (F.col(check.column) >= F.col(prev_col))
    )
