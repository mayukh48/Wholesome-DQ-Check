"""Format-consistency check: `column`'s non-null values should all use the
same one of several possible representations - flags a column silently
mixing formats, without needing to know upfront which one is "correct."

`format_patterns` is a list of regex patterns representing the distinct
acceptable formats, e.g. for a date column mixing US and European
conventions:

    format_patterns:
      - '^\\d{2}/\\d{2}/\\d{4}$'   # MM/DD/YYYY
      - '^\\d{2}-\\d{2}-\\d{4}$'   # DD-MM-YYYY

Each value is classified by the *last* pattern in the list it matches (so
list mutually-exclusive patterns - overlapping ones aren't a supported use
case), or as unrecognized if it matches none. The majority format among
recognized values becomes the baseline; anything else - a different format,
or unrecognized entirely - counts as a failure. This is different from
`regex`, which validates against one already-known-correct pattern; this
check is for when you don't know in advance which format is "the" format,
only that there should be exactly one.
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
    if not check.format_patterns:
        raise ValueError(
            f"check '{check.name}' (format_consistency) needs 'format_patterns' (a list of regex patterns)"
        )

    non_null = df.filter(F.col(check.column).isNotNull())
    total = non_null.count()
    if total == 0:
        return CheckOutcome(ok=True, message="no non-null values present", total_rows=0, failed_rows=0)

    category_col = F.lit(-1)
    for i, pattern in enumerate(check.format_patterns):
        category_col = F.when(F.col(check.column).rlike(pattern), F.lit(i)).otherwise(category_col)
    classified = non_null.withColumn("_dq_category", category_col)

    counts = classified.groupBy("_dq_category").count().collect()
    recognized = [c for c in counts if c["_dq_category"] != -1]
    unrecognized_count = sum(c["count"] for c in counts if c["_dq_category"] == -1)

    if not recognized:
        return CheckOutcome(
            ok=False,
            message=f"no rows matched any of the {len(check.format_patterns)} format_patterns",
            pass_rate=0.0,
            total_rows=total,
            failed_rows=total,
        )

    majority = max(recognized, key=lambda c: c["count"])
    majority_count = majority["count"]
    distinct_formats = len(recognized)

    pass_rate = majority_count / total
    ok = pass_rate >= check.threshold
    failed = total - majority_count
    message = f"{distinct_formats} distinct format(s) among {total} rows; {failed} disagree with the majority format"
    if unrecognized_count:
        message += f" ({unrecognized_count} unrecognized by any format_patterns)"

    return CheckOutcome(ok=ok, message=message, pass_rate=pass_rate, total_rows=total, failed_rows=failed)
