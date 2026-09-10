"""Stale-record check: `column`'s own value must be within `max_age_hours`
of now, evaluated per row - not just the dataset's most recent value.

`freshness` (dataset-level) only asks "is the newest row recent enough" -
it says nothing about individual rows that never got updated, like a
customer's address that's been stale for a year while other customers'
records update daily. This is that check, at row grain.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    if check.max_age_hours is None:
        raise ValueError(f"check '{check.name}' (stale_record) needs 'max_age_hours'")

    col = F.col(check.column)
    age_hours = (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(col)) / 3600.0
    # A null timestamp is a completeness concern, not a staleness one - let it pass here.
    return df, (col.isNull() | (age_hours <= check.max_age_hours))
