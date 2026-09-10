"""No-circular-reference check: following `parent_column` from any row in
`column` must never lead back to that same row, within `max_depth` hops
(default 20).

Catches "circular references" (A points to B, B points to A) and, as the
simplest possible case, "reports to self"/an org-hierarchy loop - a direct
self-reference (`parent_column == column` on one row) is a cycle of length
one and is caught by the same logic, not a special case.

Spark has no native recursive query support, so this is implemented as a
bounded iterative self-join: starting from each node, follow the parent
pointer one hop at a time and check whether the trail has returned to the
origin. Each hop is one more self-join, so `max_depth` directly controls
both how deep a cycle can be detected and how expensive the check is -
don't raise it past what your hierarchy could plausibly need. A hierarchy
that's genuinely acyclic but deeper than `max_depth` levels will not be
flagged as broken; it simply isn't checked past that depth.
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
    if not check.parent_column:
        raise ValueError(f"check '{check.name}' (no_circular_reference) needs 'parent_column'")
    max_depth = check.max_depth or 20

    edges = (
        df.select(F.col(check.column).alias("_dq_node"), F.col(check.parent_column).alias("_dq_parent"))
        .filter(F.col("_dq_node").isNotNull() & F.col("_dq_parent").isNotNull())
        .distinct()
    )

    frontier = edges.select(
        F.col("_dq_node").alias("_dq_origin"), F.col("_dq_parent").alias("_dq_frontier")
    )
    cyclic_origins = frontier.filter(F.col("_dq_origin") == F.col("_dq_frontier")).select(
        F.col("_dq_origin")
    )

    for _ in range(max_depth - 1):
        frontier = frontier.join(
            edges, frontier["_dq_frontier"] == edges["_dq_node"], "inner"
        ).select(frontier["_dq_origin"], edges["_dq_parent"].alias("_dq_frontier"))
        cyclic_origins = cyclic_origins.union(
            frontier.filter(F.col("_dq_origin") == F.col("_dq_frontier")).select(F.col("_dq_origin"))
        )

    cyclic_origins = cyclic_origins.distinct()
    total_nodes = df.select(check.column).filter(F.col(check.column).isNotNull()).distinct().count()
    cyclic_count = cyclic_origins.count()
    pass_rate = 1.0 if total_nodes == 0 else (total_nodes - cyclic_count) / total_nodes
    ok = pass_rate >= check.threshold
    return CheckOutcome(
        ok=ok,
        message=f"nodes_in_a_cycle={cyclic_count}/{total_nodes} (within {max_depth} hops)",
        pass_rate=pass_rate,
        total_rows=total_nodes,
        failed_rows=cyclic_count,
    )
