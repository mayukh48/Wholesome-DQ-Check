"""Derived-field check: `column`'s non-null values must equal `expression`
(a formula over other columns), within `abs_tolerance` (default 0 = exact).

Catches "total != sum of line items due to a formula error," "amount was
truncated instead of rounded," or any other computed column that drifted
from the formula that's supposed to produce it - a plain `expression`
boolean check works too, but this handles the near-equality/tolerance
arithmetic for you instead of everyone hand-rolling
`abs(a - b) <= 0.01` themselves.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    if not check.expression:
        raise ValueError(
            f"check '{check.name}' (derived_field) needs 'expression' - the formula `column` should equal"
        )

    abs_tolerance = check.abs_tolerance if check.abs_tolerance is not None else 0.0
    col = F.col(check.column)
    computed = F.expr(check.expression)
    return df, (col.isNull() | (F.abs(col - computed) <= abs_tolerance))
