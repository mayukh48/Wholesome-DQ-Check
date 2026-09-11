"""PII-exposure check: `column`'s non-null values must NOT match `pattern` -
the inverse of `regex`. `regex` validates *toward* a pattern (the value
should look like an email); this validates *away from* one (the value
should NOT look like an unmasked SSN, credit card number, etc.).

Detection only, and only over what's visible in the data itself: this can
tell you a column contains something shaped like raw PII, or that a
masking routine left some values untouched (pair it with `regex` validating
the *masked* pattern to also catch masking that produced garbled output).
It has no way to know whether the table is properly access-controlled
(Unity Catalog grants), whether the masking algorithm is cryptographically
reversible (a code/algorithm review), or who has actually queried this
column (an audit log) - none of those have a data-value signature a check
like this can see.
"""
from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..models import CheckDefinition
from ._util import require


def build_predicate(check: CheckDefinition, df: DataFrame) -> Tuple[DataFrame, Column]:
    require(check, "column")
    if not check.pattern:
        raise ValueError(f"check '{check.name}' (pii_exposure) needs 'pattern'")
    return df, (F.col(check.column).isNull() | ~F.col(check.column).rlike(check.pattern))
