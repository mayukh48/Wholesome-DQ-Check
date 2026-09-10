"""Outlier check: `column`'s non-null values must fall within `num_std_dev`
(default 3) standard deviations of the dataset's own mean.

This is a different mechanism from `anomaly`: `anomaly` compares one
aggregate number *across runs* (is today's total revenue a big swing from
recent history); this compares *individual rows within a single run*
against each other (is this one sensor reading statistically implausible
next to all the others). Catches a faulty IoT sensor, a fat-fingered manual
entry, or any other single-row value that doesn't belong.

Computes mean/stddev with one small aggregation up front (same pattern
`uniqueness` uses for its window column), then applies a static bound per
row - still one shared batched scan alongside every other row-level check.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    num_std_dev = check.num_std_dev if check.num_std_dev is not None else 3.0

    stats = df.agg(
        F.mean(F.col(check.column)).alias("_mean"), F.stddev(F.col(check.column)).alias("_std")
    ).collect()[0]
    mean, std = stats["_mean"], stats["_std"]

    col = F.col(check.column)
    if mean is None or std is None or std == 0:
        # Not enough variance (or not enough non-null data) to judge an
        # outlier against - don't false-fail every row for lack of a baseline.
        return df, F.lit(True)

    lower, upper = mean - num_std_dev * std, mean + num_std_dev * std
    return df, (col.isNull() | ((col >= lower) & (col <= upper)))
