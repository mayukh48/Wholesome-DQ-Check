"""Length check: `column`'s non-null values must have a size within
[min, max] - reuses the same fields `range` uses for numeric bounds,
applied to the value's size instead of the value itself.

For a string (or binary) column this is character length; for an array or
map column it's automatically the element count instead (`F.size`), so the
same check type also covers "inconsistent array lengths" in a semi-
structured feed - no separate check type needed for that case.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    field = next((f for f in df.schema.fields if f.name == check.column), None)
    if field is not None and isinstance(field.dataType, (ArrayType, MapType)):
        size_col = F.size(F.col(check.column))
    else:
        size_col = F.length(F.col(check.column))

    cond = F.lit(True)
    if check.min is not None:
        cond = cond & (size_col >= check.min)
    if check.max is not None:
        cond = cond & (size_col <= check.max)
    return df, (F.col(check.column).isNull() | cond)
