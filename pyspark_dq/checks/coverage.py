"""Coverage check: every key in a reference dataset must appear at least
once in this dataset - the reverse direction of `referential_integrity`
(which instead requires this dataset's keys to exist in the reference).

Use it for "expected records are absent" cases that referential_integrity
can't see: every active store should have a sales row today, every order
should have at least one order line, every SKU in the item master should
show up in the fact table. Referential integrity only proves the rows that
*do* exist aren't orphaned; it says nothing about rows that should exist but
don't.
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
        raise ValueError(f"check '{check.name}' (coverage) needs 'ref_column'")
    if ref_df is None:
        raise ValueError(f"check '{check.name}' needs ref_dfs['{check.ref_dataset}'] to be supplied")

    expected_keys = ref_df.select(F.col(check.ref_column).alias(check.column)).distinct()
    present_keys = df.select(check.column).filter(F.col(check.column).isNotNull()).distinct()
    missing = expected_keys.join(present_keys, on=check.column, how="left_anti")

    total_expected = expected_keys.count()
    missing_count = missing.count()
    covered = total_expected - missing_count
    pass_rate = 1.0 if total_expected == 0 else covered / total_expected
    ok = pass_rate >= check.threshold
    return CheckOutcome(
        ok=ok,
        message=f"missing_keys={missing_count}/{total_expected}",
        pass_rate=pass_rate,
        total_rows=total_expected,
        failed_rows=missing_count,
    )
