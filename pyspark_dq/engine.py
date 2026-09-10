"""The DQEngine ties config + checks + a DataFrame together and produces results.

Usage (as a library, e.g. from a notebook or an orchestrated pipeline task):

    from pyspark_dq.engine import DQEngine
    from pyspark_dq.config import load_config

    config = load_config("configs/customers_silver.yaml")
    engine = DQEngine(spark)
    result_df = engine.run(df, config, ref_dfs={"countries": countries_df})
    engine.write_results(result_df, "dq_results_table")
    engine.raise_on_critical_failures(result_df)  # stop the pipeline if needed

`anomaly` and `immutability` checks need a baseline of prior runs. Pass the
results table's existing rows in as `history_df` (e.g.
``spark.table("dq_results_table")``, read *before* this run's
`write_results` call) - omit it if there's no history yet or the config has
no anomaly/immutability checks.

The engine itself has no idea what check types exist - it only knows how to
run a ROW_LEVEL_CHECKS module's build_predicate() or a DATASET_LEVEL_CHECKS
module's evaluate() (see pyspark_dq/checks/__init__.py). Adding a new check
type never requires touching this file.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from . import checks
from .models import CheckResult, DatasetConfig, DQCriticalFailure

# Schema of the results table. Kept explicit (rather than inferred) so every
# run appends rows with an identical, stable schema - important for a Delta
# table that many different pipelines will append to over time.
RESULTS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("run_timestamp", TimestampType(), False),
        StructField("dataset", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("column", StringType(), True),
        StructField("status", StringType(), False),
        StructField("threshold", DoubleType(), True),
        StructField("pass_rate", DoubleType(), True),
        StructField("total_rows", LongType(), True),
        StructField("failed_rows", LongType(), True),
        StructField("message", StringType(), True),
        StructField("metric_value", DoubleType(), True),
        StructField("metric_text", StringType(), True),
    ]
)


class DQEngine:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(
        self,
        df: DataFrame,
        config: DatasetConfig,
        ref_dfs: Optional[Dict[str, DataFrame]] = None,
        history_df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """Run every check in `config` against `df` and return a results DataFrame.

        `ref_dfs` maps a reference dataset name (as referenced by a check's
        `ref_dataset`) to its DataFrame - needed if the config has any
        referential_integrity, accuracy, reconciliation, or
        cross_dataset_consistency checks.

        `history_df` supplies prior rows from the results table (matching
        RESULTS_SCHEMA) so `anomaly`/`immutability` checks have a baseline to
        compare against - needed if the config has any of those checks.
        """
        ref_dfs = ref_dfs or {}
        run_id = str(uuid.uuid4())
        run_timestamp = datetime.now(timezone.utc)

        row_level_checks = [c for c in config.checks if c.type in checks.ROW_LEVEL_TYPES]
        dataset_level_checks = [c for c in config.checks if c.type in checks.DATASET_LEVEL_TYPES]

        results: List[CheckResult] = []

        # --- Row-level checks: batched into a single aggregation ---
        # Rather than running df.filter(...).count() once per check (N full
        # scans for N checks), we build one boolean column per check and count
        # all of them in a single df.agg(...) call, so the dataset is scanned
        # only once regardless of how many row-level checks are configured.
        total_rows = df.count()
        if row_level_checks:
            working_df = df
            predicates = {}
            for check in row_level_checks:
                module = checks.ROW_LEVEL_CHECKS[check.type]
                working_df, predicate = module.build_predicate(check, working_df)
                if check.filter_expression:
                    # Rows outside the check's scope are treated as trivially
                    # passing - same convention as null-handling in range/
                    # regex/value_set - so a scoped check still batches into
                    # the one shared aggregation below.
                    scope = F.expr(check.filter_expression)
                    predicate = (~scope) | predicate
                predicates[check.name] = predicate

            agg_exprs = [
                F.sum(F.when(pred, 0).otherwise(1)).alias(f"failed__{name}")
                for name, pred in predicates.items()
            ]
            agg_row = working_df.agg(*agg_exprs).collect()[0]

            for check in row_level_checks:
                failed_rows = agg_row[f"failed__{check.name}"] or 0
                pass_rate = 1.0 if total_rows == 0 else (total_rows - failed_rows) / total_rows
                status = "PASS" if pass_rate >= check.threshold else "FAIL"
                results.append(
                    CheckResult(
                        dataset=config.dataset,
                        layer=config.layer,
                        check_name=check.name,
                        check_type=check.type,
                        severity=check.severity,
                        column=check.column or ",".join(check.columns or []),
                        status=status,
                        threshold=check.threshold,
                        pass_rate=pass_rate,
                        total_rows=total_rows,
                        failed_rows=int(failed_rows),
                        message=f"{failed_rows}/{total_rows} rows failed",
                    )
                )

        # --- Dataset-level checks: each module runs its own logic/aggregation ---
        # Every module gets the same kwargs whether it needs them or not, so
        # the engine never needs check-type-specific knowledge - it just
        # routes to the registry and lets the module validate what it needs.
        for check in dataset_level_checks:
            module = checks.DATASET_LEVEL_CHECKS[check.type]
            check_df = df.filter(F.expr(check.filter_expression)) if check.filter_expression else df
            ref_df = ref_dfs.get(check.ref_dataset) if check.ref_dataset else None

            outcome = module.evaluate(
                check,
                check_df,
                ref_df=ref_df,
                history_df=history_df,
                dataset_name=config.dataset,
            )
            results.append(
                CheckResult(
                    dataset=config.dataset,
                    layer=config.layer,
                    check_name=check.name,
                    check_type=check.type,
                    severity=check.severity,
                    column=check.column,
                    status="PASS" if outcome.ok else "FAIL",
                    threshold=check.threshold,
                    pass_rate=outcome.pass_rate,
                    total_rows=outcome.total_rows,
                    failed_rows=outcome.failed_rows,
                    message=outcome.message,
                    metric_value=outcome.metric_value,
                    metric_text=outcome.metric_text,
                )
            )

        return self._to_dataframe(results, run_id, run_timestamp)

    def _to_dataframe(
        self, results: List[CheckResult], run_id: str, run_timestamp: datetime
    ) -> DataFrame:
        """Convert the collected CheckResult objects (driver-side Python objects,
        one per check - never one per data row) into a small Spark DataFrame
        matching RESULTS_SCHEMA."""
        rows = [
            (
                run_id,
                run_timestamp,
                r.dataset,
                r.layer,
                r.check_name,
                r.check_type,
                r.severity,
                r.column,
                r.status,
                float(r.threshold),
                r.pass_rate,
                r.total_rows,
                r.failed_rows,
                r.message,
                r.metric_value,
                r.metric_text,
            )
            for r in results
        ]
        return self.spark.createDataFrame(rows, schema=RESULTS_SCHEMA)

    def write_results(
        self, result_df: DataFrame, results_table: str, results_format: str = "delta"
    ) -> None:
        """Append this run's results to the shared results table.

        Append-only by design: the table becomes a full audit history of every
        DQ run across every dataset/layer, which is what a results dashboard
        or alerting job would query against.
        """
        writer = result_df.write.format(results_format).mode("append")
        if results_format == "delta":
            # mergeSchema tolerates the results schema growing over time
            # (e.g. a future column) without breaking older pipeline runs.
            writer = writer.option("mergeSchema", "true")
        if results_table.startswith(("s3://", "s3a://", "abfss://", "dbfs:", "/", "gs://")):
            writer.save(results_table)
        else:
            writer.saveAsTable(results_table)

    def raise_on_critical_failures(self, result_df: DataFrame) -> None:
        """Raise DQCriticalFailure if any 'critical' severity check failed.

        Call this after write_results() so the failure is always recorded in
        the results table before it stops the calling pipeline - you want a
        record of *why* the pipeline halted, not just an exception in a log.
        """
        failed_rows = result_df.filter(
            (F.col("severity") == "critical") & (F.col("status") == "FAIL")
        ).collect()
        if failed_rows:
            failures = [
                CheckResult(
                    dataset=r["dataset"],
                    layer=r["layer"],
                    check_name=r["check_name"],
                    check_type=r["check_type"],
                    severity=r["severity"],
                    column=r["column"],
                    status=r["status"],
                    threshold=r["threshold"],
                    pass_rate=r["pass_rate"],
                    total_rows=r["total_rows"],
                    failed_rows=r["failed_rows"],
                    message=r["message"],
                    metric_value=r["metric_value"],
                    metric_text=r["metric_text"],
                )
                for r in failed_rows
            ]
            raise DQCriticalFailure(failures)
