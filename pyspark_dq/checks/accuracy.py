"""Accuracy check: for rows whose join key exists in a reference dataset,
verify an attribute value agrees with the reference's value for that key -
e.g. a depot exists in the master (that's `referential_integrity`'s job) AND
is mapped to the correct state (that's this check's job). Rows whose join
key has no match in the reference dataset are out of scope here - pair this
with a `referential_integrity` check on the same column to also catch those.

This is the DQ Framework doc's "Accuracy" dimension (values correctly
reflect reality), which `referential_integrity` alone doesn't cover: proving
a foreign key exists is not the same as proving the row's other attributes
are correct for that key.

Set `value_map` when the two systems use different code schemes for the
same fact - e.g. this dataset stores "Y"/"N" but the reference stores
1/0 - so "the same concept, different coding" isn't flagged as a false
mismatch. A raw value with no entry in `value_map` is treated as a mismatch
(an unrecognized code is itself an accuracy problem worth surfacing, not
something to silently skip).

Set `abs_tolerance` when the two systems' values are expected to agree only
*approximately*, not exactly - most commonly two clocks: compare a
timestamp column against a reference system's timestamp for the same
event/entity, allowing up to `abs_tolerance` seconds of drift, instead of
requiring an exact match neither clock can realistically produce. Both
sides are cast to `double` before differencing, which turns a timestamp
into Unix epoch seconds automatically - so `abs_tolerance` is in seconds
for a timestamp `match_column`, or in the column's own units for a plain
numeric one.
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
    if not check.match_column:
        raise ValueError(f"check '{check.name}' (accuracy) needs 'match_column'")
    if ref_df is None:
        raise ValueError(f"check '{check.name}' needs ref_dfs['{check.ref_dataset}'] to be supplied")

    ref_key_col = check.ref_column or check.column
    ref_match_col = check.ref_match_column or check.match_column

    left = df.select(check.column, check.match_column)
    right = ref_df.select(
        F.col(ref_key_col).alias(check.column),
        F.col(ref_match_col).alias("_dq_ref_value"),
    ).distinct()
    joined = left.join(right, on=check.column, how="inner")

    raw_value = F.col(check.match_column)
    if check.abs_tolerance is not None:
        diff = F.abs(raw_value.cast("double") - F.col("_dq_ref_value").cast("double"))
        mismatch_condition = raw_value.isNotNull() & (diff.isNull() | (diff > check.abs_tolerance))
    elif check.value_map:
        map_items = []
        for k, v in check.value_map.items():
            map_items.append(F.lit(k))
            map_items.append(F.lit(v))
        mapping = F.create_map(*map_items)
        mapped_value = mapping[raw_value]
        # A null match_column is completeness's job, not this check's - skip
        # it. But a *non-null* raw value with no entry in value_map is a
        # genuine mismatch: an unrecognized code, not a translation gap.
        mismatch_condition = raw_value.isNotNull() & (
            mapped_value.isNull() | (mapped_value != F.col("_dq_ref_value"))
        )
    else:
        mismatch_condition = raw_value.isNotNull() & (raw_value != F.col("_dq_ref_value"))

    total = joined.count()
    mismatched = joined.filter(mismatch_condition).count()
    pass_rate = 1.0 if total == 0 else (total - mismatched) / total
    ok = pass_rate >= check.threshold
    return CheckOutcome(
        ok=ok,
        message=f"mismatched_rows={mismatched}/{total} (of rows with a matching key)",
        pass_rate=pass_rate,
        total_rows=total,
        failed_rows=mismatched,
    )
