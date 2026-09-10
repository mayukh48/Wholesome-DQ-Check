"""Completeness check: `column` must be non-null.

Set `treat_blank_as_null: true` to also fail an empty/whitespace-only string
- a common disguised-missing-value pattern ("" stored instead of NULL) that
  a plain IS NOT NULL check silently passes.

Row-level: exposes build_predicate(check, df) -> (df, boolean Column), which
the engine batches together with every other row-level check into a single
aggregation per dataset.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    col = F.col(check.column)
    if check.treat_blank_as_null:
        return df, col.isNotNull() & (F.trim(col) != F.lit(""))
    return df, col.isNotNull()
