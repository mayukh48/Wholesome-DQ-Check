"""Standalone entry point so this framework can be run as its own Spark job,
e.g. as one task in an Airflow DAG or a Databricks job, without any calling
pipeline needing to import pyspark_dq as a library.

Example (spark-submit):

    spark-submit cli.py \\
        --config configs/customers_silver.yaml \\
        --input mycatalog.silver.customers \\
        --ref-table countries=mycatalog.silver.countries \\
        --results-table mycatalog.dq.dq_results

`--config` may also point at a directory of YAML files to run many datasets'
checks in one job (e.g. every table in a layer) - results from all of them
are appended to the same results table.
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from .config import load_configs
from .engine import DQEngine
from .models import DQCriticalFailure


def _read_table_or_path(spark: SparkSession, location: str, fmt: str):
    """Accept either a catalog table name (mycatalog.schema.table) or a
    storage path (s3://..., abfss://..., /mnt/...) for convenience."""
    if location.startswith(("s3://", "s3a://", "abfss://", "dbfs:", "/", "gs://")):
        return spark.read.format(fmt).load(location)
    return spark.table(location)


def _read_history(spark: SparkSession, results_table: str, results_format: str):
    """Read the results table's existing rows to serve as anomaly-check
    history, or None if it doesn't exist yet (e.g. the very first run)."""
    try:
        return _read_table_or_path(spark, results_table, results_format)
    except AnalysisException:
        return None


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PySpark data quality checks")
    parser.add_argument("--config", required=True, help="YAML config file or directory of configs")
    parser.add_argument("--input", required=True, help="Table name or path for the dataset under test")
    parser.add_argument("--input-format", default="delta", help="Format to use when --input is a path")
    parser.add_argument(
        "--ref-table",
        action="append",
        default=[],
        dest="ref_tables",
        metavar="NAME=LOCATION",
        help="Reference dataset for referential_integrity checks, e.g. countries=mycatalog.silver.countries. "
        "Repeat this flag for multiple reference datasets.",
    )
    parser.add_argument("--results-table", required=True, help="Table name or path to append results to")
    parser.add_argument("--results-format", default="delta")
    parser.add_argument(
        "--no-fail-on-critical",
        action="store_true",
        help="Record results but don't raise/exit non-zero when a critical check fails",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    spark = SparkSession.builder.appName("pyspark-dq").getOrCreate()

    configs = load_configs(args.config)
    df = _read_table_or_path(spark, args.input, args.input_format)

    # Parse "name=location" pairs into a dict of reference DataFrames, needed
    # only if a check config declares a referential_integrity check.
    ref_dfs = {}
    for pair in args.ref_tables:
        name, _, location = pair.partition("=")
        if not name or not location:
            raise ValueError(f"--ref-table must be NAME=LOCATION, got '{pair}'")
        ref_dfs[name] = _read_table_or_path(spark, location, args.input_format)

    # Read history once, before this job writes any new rows, so anomaly
    # checks compare against prior runs rather than against themselves.
    history_df = _read_history(spark, args.results_table, args.results_format)

    engine = DQEngine(spark)
    any_critical_failure = False

    for config in configs:
        result_df = engine.run(df, config, ref_dfs=ref_dfs, history_df=history_df)
        engine.write_results(result_df, args.results_table, results_format=args.results_format)
        result_df.show(truncate=False)

        if not args.no_fail_on_critical:
            try:
                engine.raise_on_critical_failures(result_df)
            except DQCriticalFailure as e:
                print(f"ERROR: {e}", file=sys.stderr)
                any_critical_failure = True

    return 1 if any_critical_failure else 0


if __name__ == "__main__":
    sys.exit(main())
