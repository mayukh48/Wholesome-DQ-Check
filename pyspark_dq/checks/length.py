"""Length check: `column`'s non-null string values must have a character
length within [min, max] - reuses the same fields `range` uses for numeric
bounds, applied to `length(column)` instead of the value itself.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    length_col = F.length(F.col(check.column))
    cond = F.lit(True)
    if check.min is not None:
        cond = cond & (length_col >= check.min)
    if check.max is not None:
        cond = cond & (length_col <= check.max)
    return df, (F.col(check.column).isNull() | cond)
