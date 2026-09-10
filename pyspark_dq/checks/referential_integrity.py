"""Referential integrity check: `column`'s non-null values must exist in a
reference dataset's `ref_column`.

Implemented as a left-anti join: rows in df (excluding nulls, which are a
completeness concern) that don't find a match in the distinct set of
reference values are "orphans" and count as failures. This only proves the
key *exists* in the reference - see `accuracy` for verifying a mapped
attribute is *correct*, not just present.
"""
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition, CheckOutcome
from ._util import require


def evaluate(
    check: CheckDefinition,
    df: DataFrame,
    *,
    ref_df: Optional[DataFrame] = None,
    history_df: Optional[DataFrame] = None,
    dataset_name: Optional[str] = None,
) -> CheckOutcome:
    require(check, "column")
    if not check.ref_column:
        raise ValueError(f"check '{check.name}' (referential_integrity) needs 'ref_column'")
    if ref_df is None:
        raise ValueError(f"check '{check.name}' needs ref_dfs['{check.ref_dataset}'] to be supplied")

    total = df.count()
    non_null = df.filter(F.col(check.column).isNotNull())
    ref_values = ref_df.select(F.col(check.ref_column).alias(check.column)).distinct()
    orphans = non_null.join(ref_values, on=check.column, how="left_anti")
    failed = orphans.count()
    pass_rate = 1.0 if total == 0 else (total - failed) / total
    ok = pass_rate >= check.threshold
    return CheckOutcome(
        ok=ok,
        message=f"orphan_rows={failed}/{total}",
        pass_rate=pass_rate,
        total_rows=total,
        failed_rows=failed,
    )
