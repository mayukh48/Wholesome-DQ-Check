"""Tests for the gap-closing check types added on top of the original nine:
reconciliation, cross_dataset_consistency, anomaly, expression, accuracy,
immutability, coverage, period_gap, and sentinel_value - plus the generic
filter_expression scoping feature and completeness's treat_blank_as_null."""
from datetime import date, datetime, timedelta, timezone

from pyspark_dq.engine import DQEngine
from pyspark_dq.models import CheckDefinition, DatasetConfig


def _run(spark, df, check, **run_kwargs):
    config = DatasetConfig(dataset="orders", layer="silver", checks=[check])
    engine = DQEngine(spark)
    result_df = engine.run(df, config, **run_kwargs)
    return result_df.collect()[0]


def test_expression_check_flags_rows_that_violate_the_rule(spark):
    df = spark.createDataFrame(
        [(1, "OPEN", 100.0), (2, "REFUNDED", -50.0), (3, "REFUNDED", 30.0)],
        ["order_id", "status", "amount"],
    )
    check = CheckDefinition(
        name="refund_amount_must_be_nonpositive",
        type="expression",
        expression="status != 'REFUNDED' OR amount <= 0",
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1
    assert result["total_rows"] == 3
    assert abs(result["pass_rate"] - 2 / 3) < 1e-9


def test_reconciliation_passes_within_threshold(spark):
    raw_df = spark.createDataFrame([(i,) for i in range(100)], ["id"])
    processed_df = spark.createDataFrame([(i,) for i in range(98)], ["id"])
    check = CheckDefinition(
        name="raw_vs_processed_counts",
        type="reconciliation",
        ref_dataset="raw",
        threshold=0.95,
    )
    result = _run(spark, processed_df, check, ref_dfs={"raw": raw_df})

    assert result["status"] == "PASS"
    assert result["metric_value"] == 98.0
    assert abs(result["pass_rate"] - 0.98) < 1e-9


def test_reconciliation_fails_when_counts_diverge_too_much(spark):
    raw_df = spark.createDataFrame([(i,) for i in range(100)], ["id"])
    processed_df = spark.createDataFrame([(i,) for i in range(98)], ["id"])
    check = CheckDefinition(
        name="raw_vs_processed_counts",
        type="reconciliation",
        ref_dataset="raw",
        threshold=1.0,
    )
    result = _run(spark, processed_df, check, ref_dfs={"raw": raw_df})

    assert result["status"] == "FAIL"


def test_cross_dataset_consistency_flags_mismatched_groups(spark):
    raw_df = spark.createDataFrame(
        [("A", 100.0), ("A", 50.0), ("B", 200.0)], ["sub_segment", "amount"]
    )
    ytd_df = spark.createDataFrame(
        [("A", 150.0), ("B", 180.0)], ["sub_segment", "amount"]
    )
    check = CheckDefinition(
        name="raw_vs_ytd_by_sub_segment",
        type="cross_dataset_consistency",
        column="amount",
        columns=["sub_segment"],
        ref_dataset="ytd",
        tolerance=0.05,
        threshold=1.0,
    )
    result = _run(spark, raw_df, check, ref_dfs={"ytd": ytd_df})

    assert result["status"] == "FAIL"
    assert "1/2 groups mismatched" in result["message"]


def test_cross_dataset_consistency_passes_when_groups_match(spark):
    raw_df = spark.createDataFrame([("A", 150.0), ("B", 180.0)], ["sub_segment", "amount"])
    ytd_df = spark.createDataFrame([("A", 150.0), ("B", 180.0)], ["sub_segment", "amount"])
    check = CheckDefinition(
        name="raw_vs_ytd_by_sub_segment",
        type="cross_dataset_consistency",
        column="amount",
        columns=["sub_segment"],
        ref_dataset="ytd",
        threshold=1.0,
    )
    result = _run(spark, raw_df, check, ref_dfs={"ytd": ytd_df})

    assert result["status"] == "PASS"


def test_anomaly_passes_with_no_history_available(spark):
    df = spark.createDataFrame([(i,) for i in range(1000)], ["id"])
    check = CheckDefinition(name="row_count_anomaly", type="anomaly")
    result = _run(spark, df, check, history_df=None)

    assert result["status"] == "PASS"
    assert result["metric_value"] == 1000.0
    assert "baseline not established" in result["message"]


def test_anomaly_fails_on_a_large_swing_against_history(spark):
    df = spark.createDataFrame([(i,) for i in range(2000)], ["id"])  # doubled overnight
    now = datetime.now(timezone.utc)
    history_df = spark.createDataFrame(
        [
            ("orders", "row_count_anomaly", now - timedelta(days=1), 1000.0),
            ("orders", "row_count_anomaly", now - timedelta(days=2), 1050.0),
            ("orders", "row_count_anomaly", now - timedelta(days=3), 980.0),
        ],
        ["dataset", "check_name", "run_timestamp", "metric_value"],
    )
    check = CheckDefinition(name="row_count_anomaly", type="anomaly", max_pct_change=0.5)
    result = _run(spark, df, check, history_df=history_df)

    assert result["status"] == "FAIL"
    assert result["metric_value"] == 2000.0


def test_anomaly_passes_within_tolerance_of_history(spark):
    df = spark.createDataFrame([(i,) for i in range(1020)], ["id"])
    now = datetime.now(timezone.utc)
    history_df = spark.createDataFrame(
        [
            ("orders", "row_count_anomaly", now - timedelta(days=1), 1000.0),
            ("orders", "row_count_anomaly", now - timedelta(days=2), 1010.0),
        ],
        ["dataset", "check_name", "run_timestamp", "metric_value"],
    )
    check = CheckDefinition(name="row_count_anomaly", type="anomaly", max_pct_change=0.1)
    result = _run(spark, df, check, history_df=history_df)

    assert result["status"] == "PASS"


def test_accuracy_flags_mismatched_reference_attribute(spark):
    # D1 exists in the master (referential_integrity would pass) but is
    # mapped to the wrong state in this data - exactly the "depot mapped to
    # wrong state" issue referential_integrity alone can't catch.
    df = spark.createDataFrame([("D1", "CA"), ("D2", "NY")], ["depot_id", "state"])
    ref_df = spark.createDataFrame([("D1", "TX"), ("D2", "NY")], ["depot_id", "state"])
    check = CheckDefinition(
        name="depot_state_matches_master",
        type="accuracy",
        column="depot_id",
        match_column="state",
        ref_dataset="depot_master",
        threshold=1.0,
    )
    result = _run(spark, df, check, ref_dfs={"depot_master": ref_df})

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1
    assert result["total_rows"] == 2


def test_accuracy_passes_when_mapping_correct(spark):
    df = spark.createDataFrame([("D1", "TX"), ("D2", "NY")], ["depot_id", "state"])
    ref_df = spark.createDataFrame([("D1", "TX"), ("D2", "NY")], ["depot_id", "state"])
    check = CheckDefinition(
        name="depot_state_matches_master",
        type="accuracy",
        column="depot_id",
        match_column="state",
        ref_dataset="depot_master",
        threshold=1.0,
    )
    result = _run(spark, df, check, ref_dfs={"depot_master": ref_df})

    assert result["status"] == "PASS"


def test_immutability_passes_with_no_history(spark):
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    check = CheckDefinition(name="historical_snapshot_stable", type="immutability")
    result = _run(spark, df, check, history_df=None)

    assert result["status"] == "PASS"
    assert result["metric_text"] is not None
    assert "baseline not established" in result["message"]


def test_immutability_detects_changed_historical_data(spark):
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    check = CheckDefinition(name="historical_snapshot_stable", type="immutability")

    # First run establishes the baseline checksum - capture it rather than
    # hardcoding an xxhash64 value.
    first = _run(spark, df, check, history_df=None)
    checksum = first["metric_text"]
    now = datetime.now(timezone.utc)

    matching_history = spark.createDataFrame(
        [("orders", "historical_snapshot_stable", now - timedelta(days=1), checksum)],
        ["dataset", "check_name", "run_timestamp", "metric_text"],
    )
    unchanged = _run(spark, df, check, history_df=matching_history)
    assert unchanged["status"] == "PASS"

    stale_history = spark.createDataFrame(
        [("orders", "historical_snapshot_stable", now - timedelta(days=1), "some-other-checksum")],
        ["dataset", "check_name", "run_timestamp", "metric_text"],
    )
    changed = _run(spark, df, check, history_df=stale_history)
    assert changed["status"] == "FAIL"
    assert "historical data changed" in changed["message"]


def test_filter_expression_scopes_a_row_level_check(spark):
    df = spark.createDataFrame(
        [(1, "ACTIVE", None), (2, "ACTIVE", "x@example.com"), (3, "INACTIVE", None)],
        ["id", "status", "email"],
    )
    check = CheckDefinition(
        name="active_rows_must_have_email",
        type="completeness",
        column="email",
        filter_expression="status = 'ACTIVE'",
        threshold=1.0,
    )
    result = _run(spark, df, check)

    # Row 3 (INACTIVE, email NULL) is out of the check's scope and must not
    # count as a failure; only row 1 (ACTIVE, email NULL) should.
    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1
    assert result["total_rows"] == 3


def test_completeness_treat_blank_as_null_flags_empty_strings(spark):
    df = spark.createDataFrame([(1, "C1"), (2, ""), (3, None)], ["id", "customer_id"])
    check = CheckDefinition(
        name="customer_id_present",
        type="completeness",
        column="customer_id",
        treat_blank_as_null=True,
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 2  # both "" and the true NULL count as missing


def test_completeness_without_flag_allows_empty_strings(spark):
    df = spark.createDataFrame([(1, "C1"), (2, ""), (3, None)], ["id", "customer_id"])
    check = CheckDefinition(
        name="customer_id_present",
        type="completeness",
        column="customer_id",
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1  # only the true NULL counts without the flag


def test_sentinel_value_flags_disallowed_placeholders(spark):
    df = spark.createDataFrame(
        [(1, 100.0), (2, 0.0), (3, -999.0), (4, None)], ["id", "amount"]
    )
    check = CheckDefinition(
        name="amount_not_sentinel",
        type="sentinel_value",
        column="amount",
        disallowed_values=[0.0, -999.0],
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 2  # 0.0 and -999.0; the real NULL is untouched
    assert result["total_rows"] == 4


def test_coverage_flags_master_keys_missing_from_fact_table(spark):
    stores_df = spark.createDataFrame([("A",), ("B",), ("C",)], ["store_id"])
    sales_df = spark.createDataFrame(
        [("A", 100.0), ("A", 50.0), ("C", 20.0)], ["store_id", "amount"]
    )
    check = CheckDefinition(
        name="every_store_has_sales_today",
        type="coverage",
        column="store_id",
        ref_dataset="stores",
        ref_column="store_id",
        threshold=1.0,
    )
    result = _run(spark, sales_df, check, ref_dfs={"stores": stores_df})

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1  # store B never shows up in sales
    assert result["total_rows"] == 3


def test_coverage_passes_when_every_key_present(spark):
    stores_df = spark.createDataFrame([("A",), ("B",)], ["store_id"])
    sales_df = spark.createDataFrame([("A", 100.0), ("B", 50.0)], ["store_id", "amount"])
    check = CheckDefinition(
        name="every_store_has_sales_today",
        type="coverage",
        column="store_id",
        ref_dataset="stores",
        ref_column="store_id",
        threshold=1.0,
    )
    result = _run(spark, sales_df, check, ref_dfs={"stores": stores_df})

    assert result["status"] == "PASS"


def test_period_gap_detects_a_missing_month(spark):
    df = spark.createDataFrame(
        [(date(2025, 1, 15),), (date(2025, 2, 10),), (date(2025, 4, 5),)],
        ["report_date"],
    )
    check = CheckDefinition(
        name="no_missing_months",
        type="period_gap",
        column="report_date",
        frequency="month",
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1  # March is missing
    assert result["total_rows"] == 4  # Jan, Feb, Mar, Apr
    assert "missing_periods=1/4" in result["message"]


def test_period_gap_passes_when_no_gaps(spark):
    df = spark.createDataFrame(
        [(date(2025, 1, 15),), (date(2025, 2, 10),), (date(2025, 3, 5),)],
        ["report_date"],
    )
    check = CheckDefinition(
        name="no_missing_months",
        type="period_gap",
        column="report_date",
        frequency="month",
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"
