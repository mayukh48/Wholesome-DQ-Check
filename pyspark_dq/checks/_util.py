"""Shared helper used by check modules. Not part of the public API - import
a check module directly (e.g. ``pyspark_dq.checks.completeness``) instead."""
from __future__ import annotations

from ..models import CheckDefinition


def require(check: CheckDefinition, attr: str) -> None:
    """Raise a clear config error early instead of a confusing Spark stack trace later."""
    if getattr(check, attr, None) is None:
        raise ValueError(f"check '{check.name}' ({check.type}) requires '{attr}'")
