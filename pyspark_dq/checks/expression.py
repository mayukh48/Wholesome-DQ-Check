"""Expression check: an arbitrary SQL boolean predicate, evaluated per row.

The generic escape hatch for business rules that don't fit any other check
type, e.g. "month(report_date) = month(current_date())".
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    if not check.expression:
        raise ValueError(f"check '{check.name}' (expression) needs 'expression'")
    return df, F.expr(check.expression)
