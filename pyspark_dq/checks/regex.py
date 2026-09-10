"""Regex check: `column`'s non-null values must match `pattern`."""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    if not check.pattern:
        raise ValueError(f"check '{check.name}' (regex) needs 'pattern'")
    return df, (F.col(check.column).isNull() | F.col(check.column).rlike(check.pattern))
