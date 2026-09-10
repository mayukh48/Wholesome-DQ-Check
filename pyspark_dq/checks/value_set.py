"""Value-set check: `column`'s non-null values must be in `allowed_values`."""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    if not check.allowed_values:
        raise ValueError(f"check '{check.name}' (value_set) needs 'allowed_values'")
    return df, (F.col(check.column).isNull() | F.col(check.column).isin(check.allowed_values))
