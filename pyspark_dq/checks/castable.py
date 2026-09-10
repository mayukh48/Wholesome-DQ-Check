"""Castable check: `column`'s non-null values must actually convert to
`target_type` (a Spark SQL type name, e.g. "date", "double", "int",
"timestamp", "boolean") - not just superficially look like it.

Uses `try_cast`, which returns NULL on a failed conversion instead of
raising, so a mismatch shows up as a normal per-row failure. This is a more
reliable and less fiddly tool than hand-writing a `regex` pattern for dates
or numbers: `try_cast(..., "date")` correctly rejects "2023-13-45" or
"2023-02-30" (not a real date) the way a format-only regex pattern can't -
matching the shape of a date isn't the same as being one.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    if not check.target_type:
        raise ValueError(
            f"check '{check.name}' (castable) needs 'target_type' (e.g. date, double, int, timestamp)"
        )
    col = F.col(check.column)
    casted = F.expr(f"try_cast({check.column} AS {check.target_type})")
    return df, (col.isNull() | casted.isNotNull())
