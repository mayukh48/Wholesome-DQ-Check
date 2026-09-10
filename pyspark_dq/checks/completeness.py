"""Completeness check: `column` must be non-null.

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
    return df, F.col(check.column).isNotNull()
