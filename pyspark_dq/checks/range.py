"""Range check: `column`'s non-null values must fall within [min, max]."""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    cond = F.lit(True)
    if check.min is not None:
        cond = cond & (F.col(check.column) >= check.min)
    if check.max is not None:
        cond = cond & (F.col(check.column) <= check.max)
    # Nulls are not a range violation (that's what completeness checks for);
    # only a non-null value that falls outside [min, max] fails this check.
    return df, (F.col(check.column).isNull() | cond)
