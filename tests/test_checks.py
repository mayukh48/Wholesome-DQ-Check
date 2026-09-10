"""Tests for the gap-closing check types added on top of the original nine:
reconciliation, cross_dataset_consistency, anomaly, expression, accuracy,
immutability, coverage, period_gap, sentinel_value, stale_record,
derived_field, outlier, scd_overlap, uniform_value, castable, length,
fuzzy_duplicate, cross_source_duplicate, monotonicity, and
no_circular_reference - plus the generic filter_expression scoping
feature, completeness's treat_blank_as_null, accuracy's
value_map/abs_tolerance, schema's strict/enforce_order, and length's
array/map size support."""
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


def test_stale_record_flags_old_per_row_timestamps(spark):
    now = datetime.now(timezone.utc)
    df = spark.createDataFrame(
        [(1, now - timedelta(hours=1)), (2, now - timedelta(hours=48)), (3, None)],
        ["id", "updated_at"],
    )
    check = CheckDefinition(
        name="customer_record_is_fresh",
        type="stale_record",
        column="updated_at",
        max_age_hours=24,
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1  # only the 48h-old row fails; NULL is completeness's job
    assert result["total_rows"] == 3


def test_stale_record_passes_when_all_rows_are_recent(spark):
    now = datetime.now(timezone.utc)
    df = spark.createDataFrame(
        [(1, now - timedelta(hours=1)), (2, now - timedelta(hours=2))],
        ["id", "updated_at"],
    )
    check = CheckDefinition(
        name="customer_record_is_fresh",
        type="stale_record",
        column="updated_at",
        max_age_hours=24,
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_derived_field_flags_incorrect_totals(spark):
    df = spark.createDataFrame(
        [(1, 10.0, 2.0, 20.0), (2, 5.0, 3.0, 14.0)],  # row 2: 5*3=15, not 14
        ["id", "unit_price", "quantity", "total"],
    )
    check = CheckDefinition(
        name="total_matches_unit_price_times_quantity",
        type="derived_field",
        column="total",
        expression="unit_price * quantity",
        abs_tolerance=0.01,
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1
    assert result["total_rows"] == 2


def test_derived_field_passes_within_tolerance(spark):
    df = spark.createDataFrame([(1, 10.0, 2.0, 20.001)], ["id", "unit_price", "quantity", "total"])
    check = CheckDefinition(
        name="total_matches_unit_price_times_quantity",
        type="derived_field",
        column="total",
        expression="unit_price * quantity",
        abs_tolerance=0.01,
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_outlier_flags_a_statistically_implausible_reading(spark):
    normal_readings = [(i, 20.0 + (i % 3) * 0.5) for i in range(20)]  # clustered ~20.0-21.0
    df = spark.createDataFrame(normal_readings + [(999, 500.0)], ["sensor_id", "reading"])
    check = CheckDefinition(
        name="sensor_reading_not_an_outlier",
        type="outlier",
        column="reading",
        num_std_dev=3.0,
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1
    assert result["total_rows"] == 21


def test_outlier_passes_when_no_baseline_variance(spark):
    # A single-row (or zero-variance) dataset has no baseline to judge an
    # outlier against - this must not false-fail for lack of data.
    df = spark.createDataFrame([(1, 20.0)], ["sensor_id", "reading"])
    check = CheckDefinition(name="sensor_reading_not_an_outlier", type="outlier", column="reading")
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_accuracy_value_map_translates_before_comparing(spark):
    df = spark.createDataFrame([("C1", "Y"), ("C2", "N")], ["customer_id", "is_active"])
    ref_df = spark.createDataFrame([("C1", 1), ("C2", 0)], ["customer_id", "active_flag"])
    check = CheckDefinition(
        name="is_active_matches_crm",
        type="accuracy",
        column="customer_id",
        match_column="is_active",
        ref_dataset="crm",
        ref_match_column="active_flag",
        value_map={"Y": 1, "N": 0},
        threshold=1.0,
    )
    result = _run(spark, df, check, ref_dfs={"crm": ref_df})

    assert result["status"] == "PASS"


def test_accuracy_value_map_flags_real_mismatch(spark):
    df = spark.createDataFrame([("C1", "Y"), ("C2", "N")], ["customer_id", "is_active"])
    ref_df = spark.createDataFrame([("C1", 0), ("C2", 0)], ["customer_id", "active_flag"])  # C1 disagrees
    check = CheckDefinition(
        name="is_active_matches_crm",
        type="accuracy",
        column="customer_id",
        match_column="is_active",
        ref_dataset="crm",
        ref_match_column="active_flag",
        value_map={"Y": 1, "N": 0},
        threshold=1.0,
    )
    result = _run(spark, df, check, ref_dfs={"crm": ref_df})

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1


def test_scd_overlap_flags_overlapping_effective_ranges(spark):
    df = spark.createDataFrame(
        [
            ("E1", date(2024, 1, 1), date(2024, 6, 1)),
            ("E1", date(2024, 3, 1), None),  # overlaps E1's first row
            ("E2", date(2024, 1, 1), date(2024, 6, 1)),
            ("E2", date(2024, 6, 1), None),  # starts exactly when E2's first row ends - no overlap
        ],
        ["entity_id", "valid_from", "valid_to"],
    )
    check = CheckDefinition(
        name="no_overlapping_customer_versions",
        type="scd_overlap",
        columns=["entity_id"],
        start_column="valid_from",
        end_column="valid_to",
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1  # only E1 has an overlap
    assert result["total_rows"] == 2  # 2 distinct entities total


def test_scd_overlap_passes_with_clean_ranges(spark):
    df = spark.createDataFrame(
        [
            ("E2", date(2024, 1, 1), date(2024, 6, 1)),
            ("E2", date(2024, 6, 1), None),
        ],
        ["entity_id", "valid_from", "valid_to"],
    )
    check = CheckDefinition(
        name="no_overlapping_customer_versions",
        type="scd_overlap",
        columns=["entity_id"],
        start_column="valid_from",
        end_column="valid_to",
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_uniform_value_flags_mixed_currencies(spark):
    df = spark.createDataFrame([(1, "USD"), (2, "USD"), (3, "EUR")], ["id", "currency"])
    check = CheckDefinition(
        name="currency_is_uniform", type="uniform_value", column="currency", threshold=1.0
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1  # the one EUR row disagrees with the majority (USD)
    assert result["total_rows"] == 3


def test_uniform_value_passes_when_all_same(spark):
    df = spark.createDataFrame([(1, "USD"), (2, "USD")], ["id", "currency"])
    check = CheckDefinition(
        name="currency_is_uniform", type="uniform_value", column="currency", threshold=1.0
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_uniform_value_with_explicit_expected_value(spark):
    df = spark.createDataFrame([(1, "USD"), (2, "EUR")], ["id", "currency"])
    check = CheckDefinition(
        name="currency_is_usd",
        type="uniform_value",
        column="currency",
        allowed_values=["USD"],
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1


def test_castable_flags_unparseable_values(spark):
    df = spark.createDataFrame(
        [(1, "2023-05-10"), (2, "13/45/2023"), (3, None)], ["id", "raw_date"]
    )
    check = CheckDefinition(
        name="raw_date_is_a_real_date",
        type="castable",
        column="raw_date",
        target_type="date",
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1  # only "13/45/2023"; NULL is completeness's job
    assert result["total_rows"] == 3


def test_castable_rejects_a_logically_invalid_date(spark):
    # "2023-02-30" is shaped like a date but isn't a real calendar date - a
    # format-only regex pattern would wrongly let this through.
    df = spark.createDataFrame([(1, "2023-02-30")], ["id", "raw_date"])
    check = CheckDefinition(
        name="raw_date_is_a_real_date", type="castable", column="raw_date", target_type="date"
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1


def test_castable_passes_valid_numeric_strings(spark):
    df = spark.createDataFrame([(1, "42.5"), (2, "-3")], ["id", "raw_amount"])
    check = CheckDefinition(
        name="raw_amount_is_numeric", type="castable", column="raw_amount", target_type="double"
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_length_flags_out_of_bounds_strings(spark):
    df = spark.createDataFrame([(1, "AB"), (2, "ABCDE"), (3, None)], ["id", "code"])
    check = CheckDefinition(
        name="code_length_is_valid", type="length", column="code", min=3, max=4, threshold=1.0
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 2  # "AB" too short, "ABCDE" too long; NULL is completeness's job
    assert result["total_rows"] == 3


def test_length_passes_within_bounds(spark):
    df = spark.createDataFrame([(1, "ABC"), (2, "ABCD")], ["id", "code"])
    check = CheckDefinition(name="code_length_is_valid", type="length", column="code", min=3, max=4)
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_fuzzy_duplicate_flags_near_match_names(spark):
    df = spark.createDataFrame(
        [(1, "Jon Smith"), (2, "John Smith"), (3, "Alice Brown")], ["id", "full_name"]
    )
    check = CheckDefinition(
        name="no_near_duplicate_names",
        type="fuzzy_duplicate",
        column="full_name",
        max_edit_distance=2,
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 2  # "Jon Smith" and "John Smith" flag each other
    assert result["total_rows"] == 3


def test_fuzzy_duplicate_passes_when_no_names_are_close(spark):
    df = spark.createDataFrame([(1, "Alice Brown"), (2, "Bob Jones")], ["id", "full_name"])
    check = CheckDefinition(
        name="no_near_duplicate_names", type="fuzzy_duplicate", column="full_name", max_edit_distance=2
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_fuzzy_duplicate_uses_blocking_key_to_scope_comparisons(spark):
    df = spark.createDataFrame(
        [(1, "Jon Smith", "10001"), (2, "John Smith", "20002")], ["id", "full_name", "postal_code"]
    )
    check = CheckDefinition(
        name="no_near_duplicate_names",
        type="fuzzy_duplicate",
        column="full_name",
        columns=["postal_code"],  # different postal codes - never compared, so no flag despite similar names
        max_edit_distance=2,
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_cross_source_duplicate_flags_overlapping_keys(spark):
    web_signups = spark.createDataFrame([("C1",), ("C2",), ("C3",)], ["customer_id"])
    store_signups = spark.createDataFrame([("C2",), ("C4",)], ["customer_id"])
    check = CheckDefinition(
        name="no_overlap_between_web_and_store_signups",
        type="cross_source_duplicate",
        column="customer_id",
        ref_dataset="store_signups",
        ref_column="customer_id",
        threshold=1.0,
    )
    result = _run(spark, web_signups, check, ref_dfs={"store_signups": store_signups})

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1  # C2 is in both
    assert result["total_rows"] == 3


def test_cross_source_duplicate_passes_when_disjoint(spark):
    batch = spark.createDataFrame([("O1",), ("O2",)], ["order_id"])
    already_loaded = spark.createDataFrame([("O3",), ("O4",)], ["order_id"])
    check = CheckDefinition(
        name="backfill_does_not_reinsert_loaded_rows",
        type="cross_source_duplicate",
        column="order_id",
        ref_dataset="already_loaded",
        ref_column="order_id",
        threshold=1.0,
    )
    result = _run(spark, batch, check, ref_dfs={"already_loaded": already_loaded})

    assert result["status"] == "PASS"


def test_monotonicity_flags_out_of_order_events(spark):
    df = spark.createDataFrame(
        [
            ("D1", 1, datetime(2024, 1, 1, 10, 0)),
            ("D1", 2, datetime(2024, 1, 1, 9, 30)),  # out of order vs. arrival_seq=1
            ("D1", 3, datetime(2024, 1, 1, 10, 15)),
            ("D2", 1, datetime(2024, 1, 1, 8, 0)),
            ("D2", 2, datetime(2024, 1, 1, 8, 5)),
        ],
        ["device_id", "arrival_seq", "event_time"],
    )
    check = CheckDefinition(
        name="events_arrive_in_order",
        type="monotonicity",
        column="event_time",
        order_by="arrival_seq",
        columns=["device_id"],
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1
    assert result["total_rows"] == 5


def test_monotonicity_passes_when_events_are_in_order(spark):
    df = spark.createDataFrame(
        [
            ("D1", 1, datetime(2024, 1, 1, 10, 0)),
            ("D1", 2, datetime(2024, 1, 1, 10, 5)),
        ],
        ["device_id", "arrival_seq", "event_time"],
    )
    check = CheckDefinition(
        name="events_arrive_in_order",
        type="monotonicity",
        column="event_time",
        order_by="arrival_seq",
        columns=["device_id"],
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_accuracy_abs_tolerance_allows_small_clock_drift(spark):
    now = datetime.now(timezone.utc)
    df = spark.createDataFrame([("E1", now)], ["event_id", "local_ts"])
    ref_df = spark.createDataFrame([("E1", now + timedelta(seconds=30))], ["event_id", "source_ts"])
    check = CheckDefinition(
        name="clocks_agree_within_tolerance",
        type="accuracy",
        column="event_id",
        match_column="local_ts",
        ref_dataset="source_system",
        ref_match_column="source_ts",
        abs_tolerance=60,  # seconds
        threshold=1.0,
    )
    result = _run(spark, df, check, ref_dfs={"source_system": ref_df})

    assert result["status"] == "PASS"


def test_accuracy_abs_tolerance_flags_significant_drift(spark):
    now = datetime.now(timezone.utc)
    df = spark.createDataFrame([("E1", now)], ["event_id", "local_ts"])
    ref_df = spark.createDataFrame([("E1", now + timedelta(minutes=10))], ["event_id", "source_ts"])
    check = CheckDefinition(
        name="clocks_agree_within_tolerance",
        type="accuracy",
        column="event_id",
        match_column="local_ts",
        ref_dataset="source_system",
        ref_match_column="source_ts",
        abs_tolerance=60,
        threshold=1.0,
    )
    result = _run(spark, df, check, ref_dfs={"source_system": ref_df})

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1


def test_no_circular_reference_flags_self_reference(spark):
    df = spark.createDataFrame([("E1", "E1"), ("E2", "E1")], ["employee_id", "manager_id"])
    check = CheckDefinition(
        name="no_org_hierarchy_loop",
        type="no_circular_reference",
        column="employee_id",
        parent_column="manager_id",
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1  # E1 reports to itself
    assert result["total_rows"] == 2


def test_no_circular_reference_flags_a_multi_hop_cycle(spark):
    df = spark.createDataFrame(
        [("A", "B"), ("B", "C"), ("C", "A")], ["category_id", "parent_category_id"]
    )
    check = CheckDefinition(
        name="no_category_hierarchy_loop",
        type="no_circular_reference",
        column="category_id",
        parent_column="parent_category_id",
        max_depth=5,
        threshold=1.0,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 3  # A, B, and C are all part of the cycle


def test_no_circular_reference_passes_for_a_clean_tree(spark):
    df = spark.createDataFrame(
        [("A", None), ("B", "A"), ("C", "A"), ("D", "B")], ["category_id", "parent_category_id"]
    )
    check = CheckDefinition(
        name="no_category_hierarchy_loop",
        type="no_circular_reference",
        column="category_id",
        parent_column="parent_category_id",
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_no_circular_reference_respects_max_depth_bound(spark):
    # A 4-hop cycle (A->B->C->D->A) isn't detected with max_depth=2 - a
    # documented characteristic, not a bug: it simply isn't checked that deep.
    df = spark.createDataFrame(
        [("A", "B"), ("B", "C"), ("C", "D"), ("D", "A")], ["category_id", "parent_category_id"]
    )
    check = CheckDefinition(
        name="no_category_hierarchy_loop",
        type="no_circular_reference",
        column="category_id",
        parent_column="parent_category_id",
        max_depth=2,
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_schema_strict_flags_unexpected_extra_columns(spark):
    df = spark.createDataFrame([(1, "A", "extra")], ["id", "name", "mystery_column"])
    check = CheckDefinition(
        name="orders_schema_is_exact",
        type="schema",
        expected_schema={"id": "bigint", "name": "string"},
        strict=True,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert "mystery_column" in result["message"]


def test_schema_without_strict_allows_extra_columns(spark):
    df = spark.createDataFrame([(1, "A", "extra")], ["id", "name", "mystery_column"])
    check = CheckDefinition(
        name="orders_schema_is_exact",
        type="schema",
        expected_schema={"id": "bigint", "name": "string"},
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_schema_enforce_order_flags_reordered_columns(spark):
    df = spark.createDataFrame([("A", 1)], ["name", "id"])  # reversed vs. declared order
    check = CheckDefinition(
        name="orders_column_order_is_stable",
        type="schema",
        expected_schema={"id": "bigint", "name": "string"},
        enforce_order=True,
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert "column order mismatch" in result["message"]


def test_schema_enforce_order_passes_when_order_matches(spark):
    df = spark.createDataFrame([(1, "A")], ["id", "name"])
    check = CheckDefinition(
        name="orders_column_order_is_stable",
        type="schema",
        expected_schema={"id": "bigint", "name": "string"},
        enforce_order=True,
    )
    result = _run(spark, df, check)

    assert result["status"] == "PASS"


def test_length_checks_array_size_not_character_length(spark):
    df = spark.createDataFrame([(1, ["a", "b", "c"]), (2, ["x"])], ["id", "tags"])
    check = CheckDefinition(
        name="tags_array_size_is_valid", type="length", column="tags", min=2, max=5, threshold=1.0
    )
    result = _run(spark, df, check)

    assert result["status"] == "FAIL"
    assert result["failed_rows"] == 1  # ["x"] has only 1 element, below min=2
    assert result["total_rows"] == 2


def test_length_array_size_passes_within_bounds(spark):
    df = spark.createDataFrame([(1, ["a", "b"]), (2, ["x", "y", "z"])], ["id", "tags"])
    check = CheckDefinition(name="tags_array_size_is_valid", type="length", column="tags", min=2, max=5)
    result = _run(spark, df, check)

    assert result["status"] == "PASS"
