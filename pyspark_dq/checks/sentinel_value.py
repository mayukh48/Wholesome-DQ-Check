"""Sentinel-value check: `column`'s non-null values must NOT be in
`disallowed_values` - the inverse of `value_set`.

Catches a NULL that was silently replaced with a placeholder before it ever
reached this table - 0, "N/A", "9999-12-31", etc. - which a plain
completeness check can't see, because the value genuinely isn't null.
Nulls themselves are out of scope here; that's `completeness`'s job.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    if not check.disallowed_values:
        raise ValueError(f"check '{check.name}' (sentinel_value) needs 'disallowed_values'")
    return df, (F.col(check.column).isNull() | ~F.col(check.column).isin(check.disallowed_values))
