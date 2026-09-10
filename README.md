# pyspark_dq

A reusable, config-driven data quality framework for PySpark. Point it at
any table at any medallion layer (bronze/silver/gold) - the layer is just
metadata attached to results, it never changes check behavior. Checks are
declared in YAML, run by a small engine against a DataFrame, and the results
land as rows in an append-only Delta table you can dashboard, alert on, or
gate a pipeline with.

This copy lives in the Databricks Workspace at:

```
/Workspace/Users/mkh.mukherjee@gmail.com/pyspark-dataquality
```

This document is written so a developer who has never seen this repo can go
from zero to a working check in a few minutes, then use the dimension-by-
dimension reference below as a lookup whenever they need a check type they
haven't used yet.

---

## Contents

1. [5-minute quickstart](#5-minute-quickstart)
2. [Core concepts](#core-concepts)
3. [The six DQ dimensions, with a worked example of every check](#the-six-dq-dimensions-with-a-worked-example-of-every-check)
4. [Cross-cutting features](#cross-cutting-features) - `filter_expression`, severity/threshold, `ref_dfs`, `history_df`
5. [Full `CheckDefinition` field reference](#full-checkdefinition-field-reference)
6. [Writing and loading configs](#writing-and-loading-configs)
7. [Running it](#running-it) - notebook, Databricks Job, CLI
8. [The results table](#the-results-table)
9. [Extending the framework](#extending-the-framework)
10. [Running the tests](#running-the-tests)
11. [Known Workspace Files gotchas](#known-workspace-files-gotchas)
12. [Quick-reference table](#quick-reference-table)

---

## 5-minute quickstart

Paste this into a Databricks notebook cell (works on serverless or classic
compute):

```python
import sys

# Workspace Files' FUSE mount rejects __pycache__ writes - set this before
# importing pyspark_dq or anything breaks with a confusing OSError. See
# "Known Workspace Files gotchas" at the bottom of this doc.
sys.dont_write_bytecode = True

PKG_ROOT = "/Workspace/Users/mkh.mukherjee@gmail.com/pyspark-dataquality"
if PKG_ROOT not in sys.path:
    sys.path.insert(0, PKG_ROOT)

from pyspark_dq import DQEngine, DatasetConfig, CheckDefinition

# A config can be built in Python (as here) or loaded from YAML - see
# "Writing and loading configs" below. Either way it's the same DatasetConfig.
config = DatasetConfig(
    dataset="customers",
    layer="silver",
    checks=[
        CheckDefinition(name="customer_id_not_null", type="completeness",
                         column="customer_id", severity="critical", threshold=1.0),
        CheckDefinition(name="customer_id_unique", type="uniqueness",
                         columns=["customer_id"], severity="critical", threshold=1.0),
        CheckDefinition(name="status_is_known_value", type="value_set",
                         column="status", allowed_values=["ACTIVE", "INACTIVE", "PENDING"],
                         severity="warning", threshold=1.0),
    ],
)

df = spark.table("mycatalog.silver.customers")

engine = DQEngine(spark)
result_df = engine.run(df, config)
display(result_df)  # one row per check: PASS/FAIL, pass_rate, message, ...

engine.write_results(result_df, "mycatalog.dq.dq_results")   # append to the shared results table
engine.raise_on_critical_failures(result_df)                  # raises DQCriticalFailure if any critical check failed
```

That's the whole mental model: build (or load) a `DatasetConfig`, hand it to
`DQEngine.run()` with a DataFrame, get back a small results DataFrame. Every
other feature in this document builds on exactly this.

---

## Core concepts

| Concept | What it is |
|---|---|
| `DatasetConfig` | `dataset` (name) + `layer` (bronze/silver/gold, pure metadata) + a list of `CheckDefinition`s. One per table you validate. |
| `CheckDefinition` | One rule: a `type` (see the dimension reference below), a `name` (must be unique within the config - it's the join key back to history for `anomaly`/`immutability`), a `severity`, a `threshold`, and whatever fields that check type needs. |
| `severity` | `"critical"` or `"warning"`. Both get recorded; only `critical` failures trigger `raise_on_critical_failures()`. |
| `threshold` | The minimum pass rate (0.0-1.0) required for `status = PASS`. `1.0` means zero tolerance; `0.98` allows up to 2% of rows (or groups, or count drift) to be bad. |
| `DQEngine` | Stateless except for holding a `SparkSession`. `run()` executes every check in a config against a DataFrame and returns a results DataFrame; `write_results()` appends it to a Delta table; `raise_on_critical_failures()` raises if anything critical failed. |
| `CheckResult` / results table row | One row per check per run: status, pass_rate, total_rows, failed_rows, a human-readable message, plus `metric_value`/`metric_text` for checks that report a number or a fingerprint. Full schema in [The results table](#the-results-table). |
| `ref_dfs` | A `{name: DataFrame}` dict passed to `run()`, for checks that compare against a second dataset (`referential_integrity`, `accuracy`, `reconciliation`, `cross_dataset_consistency`). |
| `history_df` | Prior rows from the results table, passed to `run()`, for checks that need a baseline from past runs (`anomaly`, `immutability`). |

---

## The six DQ dimensions, with a worked example of every check

Every check type maps to one of the six standard DQ dimensions this
framework is organized around. Each entry below is copy-paste runnable
(assuming the columns named exist on your DataFrame) and states which real
data issue it exists to catch.

### Completeness — no missing required fields or records

**`completeness`** - a column must be non-null. Set `treat_blank_as_null:
true` to also fail an empty/whitespace-only string - a common disguised-
missing-value pattern (`""` stored instead of a true `NULL`) that a plain
`IS NOT NULL` check silently passes.

```yaml
- name: order_id_present
  type: completeness
  column: order_id
  severity: critical
  threshold: 1.0             # 100% of rows must have a non-null order_id
  treat_blank_as_null: true   # also fail "" - optional, default false
```
Catches: blank/NULL mandatory fields (e.g. a NULL depot code), and - with
the flag - a source system writing `""` instead of `NULL`.

**`sentinel_value`** - a column's non-null values must NOT be in
`disallowed_values`. The inverse of `value_set`.

```yaml
- name: amount_not_sentinel_value
  type: sentinel_value
  column: amount
  disallowed_values: [-1, 9999999]   # known placeholders the source uses instead of a true NULL
  severity: warning
  threshold: 1.0
```
Catches: a NULL that was silently replaced with a placeholder before it
ever reached this table - `0`, `"N/A"`, `"9999-12-31"`, etc. - which
`completeness` can't see, because the value genuinely isn't null. A real
`NULL` still passes this check; that's `completeness`'s job, not this one's.

**`coverage`** - every key in a reference dataset must appear at least once
in this dataset. The reverse direction of `referential_integrity`.

```yaml
- name: every_active_store_has_orders_today
  type: coverage
  column: store_id
  ref_dataset: active_stores   # must be a key in the ref_dfs dict / --ref-table
  ref_column: store_id
  severity: warning
  threshold: 1.0
```
Catches: expected rows that are simply absent - a store with zero sales
rows on a business day, an order with no order lines. `referential_integrity`
only proves the rows that *do* exist aren't orphaned; it says nothing about
rows that should exist but don't.

**`period_gap`** - detects a missing period (day/month/year) inside the
observed range of a date column - a hole in an otherwise continuous time
series.

```yaml
- name: no_missing_months_in_trend
  type: period_gap
  column: report_date
  frequency: month    # one of day / month / year (default: day)
  severity: critical
  threshold: 1.0
```
Catches: a missing month in a five-year trend, or any other gap in a
sequence that `freshness` (only cares about the *latest* value) and
`row_count` (can't see a hole in the middle of a populated table) both miss.

**`row_count`** - the table's row count must fall within `min`/`max`.

```yaml
- name: orders_landed
  type: row_count
  min: 1                  # fail if the ingestion produced zero rows
  severity: critical
```
Catches: an empty or unexpectedly huge load (pair `min` and `max` for a
tighter volume floor/ceiling, e.g. `min: 1000, max: 200000`).

### Uniqueness — no unintended duplicate records

**`uniqueness`** - no duplicate values in `column` (or the combination in
`columns`, for a composite key).

```yaml
- name: customer_id_unique
  type: uniqueness
  columns: [customer_id]   # use `columns` (list) for a composite key; `column` also works for a single key
  severity: critical
  threshold: 1.0
```
Catches: the same outlet/customer/order appearing twice. Rows with a null
key are excluded (that's `completeness`'s job) so they never count as
duplicates.

### Validity — data conforms to expected format/type

**`range`** - a numeric column's non-null values must fall in `[min, max]`.

```yaml
- name: age_within_range
  type: range
  column: age
  min: 0
  max: 120
  severity: warning
  threshold: 0.99          # allow up to 1% of rows to have a bad age before flagging FAIL
```

**`value_set`** - a column's non-null values must be in an allow-list.

```yaml
- name: status_is_known_value
  type: value_set
  column: status
  allowed_values: [ACTIVE, INACTIVE, PENDING]
  severity: warning
  threshold: 1.0
```

**`regex`** - a column's non-null values must match a pattern.

```yaml
- name: email_looks_valid
  type: regex
  column: email
  pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
  severity: warning
  threshold: 0.98
```

**`schema`** - the DataFrame's actual dtypes must match an expected map.
Metadata-only (`df.dtypes`, no Spark job triggered), so this is essentially
free.

```yaml
- name: expected_columns_present
  type: schema
  expected_schema:
    order_id: string
    customer_id: string
    amount: double
    status: string
    created_at: timestamp
  severity: critical
```
Catches: a source column silently renamed, retyped, or dropped.

**`expression`** - any SQL boolean predicate, evaluated per row. The
open-ended escape hatch for business rules that don't fit another type.

```yaml
- name: current_month_data_only
  type: expression
  expression: "month(report_date) = month(current_date()) AND year(report_date) = year(current_date())"
  severity: critical
  threshold: 1.0
```
Catches: e.g. a market-share report accidentally including last month's rows.

### Accuracy — values correctly reflect reality

**`referential_integrity`** - a foreign key's non-null values must *exist*
in a reference dataset.

```yaml
- name: country_code_exists
  type: referential_integrity
  column: country_code
  ref_dataset: countries       # must be a key in the ref_dfs dict / --ref-table
  ref_column: code
  severity: critical
  threshold: 1.0
```
Catches: a country_code with no corresponding master record.

**`accuracy`** - for rows whose join key *does* exist in a reference
dataset, verify a mapped attribute *agrees* with the reference's value for
that key. `referential_integrity` alone can't catch this: a key existing
doesn't mean the row's other attributes were mapped correctly.

```yaml
- name: country_region_matches_master
  type: accuracy
  column: country_code          # join key
  match_column: region           # attribute that must agree with the master's value
  ref_dataset: countries        # must be a key in the ref_dfs dict / --ref-table
  # ref_match_column defaults to match_column, ref_column defaults to column,
  # if the reference dataset uses the same names.
  severity: critical
  threshold: 1.0
```
Catches: "depot mapped to the wrong state" - the depot is a valid depot, it's
just linked to the wrong state.

### Consistency — the same fact matches across tables/reports

**`reconciliation`** - a row count (or a summed column) must match a
reference dataset, within a tolerance set by `threshold`. Doubles as an
idempotency guard.

```yaml
- name: raw_vs_silver_row_count
  type: reconciliation
  ref_dataset: raw_customers    # must be a key in the ref_dfs dict / --ref-table
  # optional: column: total_amount   -> compares sum(column) instead of row counts
  severity: critical
  threshold: 0.99                # allow up to 1% row loss between raw and cleansed, e.g. from dedup
```
Catches: rows silently dropped (or duplicated) between two stages of the
pipeline - "raw vs. processed record counts don't match."

**`cross_dataset_consistency`** - a summed column, grouped by one or more
dimensions, must match a reference dataset per group.

```yaml
- name: revenue_by_region_matches_raw_orders
  type: cross_dataset_consistency
  column: total_revenue
  columns: [region]        # group by - compares sum(total_revenue) per region
  ref_dataset: raw_orders  # must be a key in the ref_dfs dict / --ref-table
  tolerance: 0.01            # allow up to 1% rounding drift per group (default: exact match)
  severity: critical
  threshold: 1.0              # fraction of groups that must be within tolerance to pass overall
```
Catches: "raw vs. YTD sub-segment mismatch" - the region-level totals in a
downstream aggregate silently drifted from the source.

**`immutability`** - fingerprint the dataset (or, usually, a historical
slice of it via `filter_expression`) and compare against the fingerprint
recorded for this check in the last run.

```yaml
- name: historical_months_unchanged
  type: immutability
  columns: [region, total_revenue]   # fingerprint just these columns, not audit/ingestion metadata
  filter_expression: "report_date < date_trunc('month', current_date())"  # only already-finalized months
  severity: critical
```
Catches: someone silently re-ran a pipeline over a past month and changed
numbers that should have been frozen - current-period rows are expected to
change and are excluded by `filter_expression`, so they never trip this.

### Timeliness — data arrives and refreshes within SLA

**`freshness`** - the most recent timestamp in a column isn't older than
`max_age_hours`.

```yaml
- name: report_is_fresh
  type: freshness
  column: report_date
  max_age_hours: 30      # allow some slack past the 24h reporting cycle
  severity: critical
```
Catches: a scheduled job that ran but didn't actually pick up new data
(stale source), *if* this check itself is run on a schedule independent of
whether the upstream job succeeded - see the note in
["What's still out of scope, on purpose"](#whats-still-out-of-scope-on-purpose).

**`anomaly`** - a summed column (or row count) shouldn't swing more than
`max_pct_change` versus the average of the last `lookback` runs. Bridges
Timeliness ("did today's refresh look like a normal refresh") and the DQ
Framework doc's separate Volume & Anomaly Detection control (Gold-layer DQ
Gate 3).

```yaml
- name: total_revenue_not_anomalous
  type: anomaly
  column: total_revenue
  max_pct_change: 0.25    # flag if total revenue swings more than 25% vs. the recent baseline
  lookback: 7               # average of the last 7 runs (default: 5)
  severity: warning
```
Catches: "industry numbers shifted after refresh" / a market-share figure
that doubled overnight. Passes automatically until enough run history has
accumulated - a brand-new dataset never false-fails on day one.

---

## Cross-cutting features

### `filter_expression` — scope any check to a row subset

Every check type accepts an optional `filter_expression` (a SQL boolean
string). Rows outside the expression are treated as trivially passing, so
they never count as failures - the same convention this framework already
uses for nulls in `range`/`regex`/`value_set`.

```yaml
- name: active_customers_must_have_email
  type: completeness
  column: email
  filter_expression: "status = 'ACTIVE'"   # inactive customers are exempt
  severity: warning
  threshold: 1.0
```

This is what makes one config usable across a whole table without carving
it into multiple configs, and it's how `immutability` scopes itself to a
historical slice (see above).

### Severity and threshold

- `severity: critical` - failure is recorded *and* raises `DQCriticalFailure`
  from `raise_on_critical_failures()`, which pipelines typically use to halt.
- `severity: warning` - failure is recorded for a dashboard but never raises.
- `threshold` is always "the minimum pass rate to still be a PASS," on a
  0.0-1.0 scale, for every check type - whether "pass rate" means % of rows,
  % of matching groups, or an exact-match ratio for `reconciliation`.

### `ref_dfs` — checks that compare against a second dataset

`referential_integrity`, `accuracy`, `reconciliation`,
`cross_dataset_consistency`, and `coverage` all read `check.ref_dataset` as
a key into a `ref_dfs` dict you pass to `run()`:

```python
engine.run(df, config, ref_dfs={
    "countries": countries_df,
    "raw_customers": raw_customers_df,
})
```

On the CLI this is `--ref-table NAME=LOCATION`, repeatable.

### `history_df` — checks that need prior-run history

`anomaly` and `immutability` read `history_df` - prior rows from the results
table, filtered internally by `dataset` and `check_name`:

```python
history_df = spark.table("mycatalog.dq.dq_results")   # read BEFORE write_results() for this run
result_df = engine.run(df, config, history_df=history_df)
engine.write_results(result_df, "mycatalog.dq.dq_results")
```

Read it *before* writing this run's results, so the baseline never includes
today's own numbers. The CLI does this automatically from `--results-table`.

### What's still out of scope, on purpose

Two things a first gap-analysis pass flagged are *not* implemented here -
not because they're hard to code, but because they're not this library's
job. This framework only ever runs against a DataFrame that already exists;
it cannot detect a file that never arrived, or reject a malformed file
before it lands. Those need Databricks Workflows (SLA alerting on job/task
runs) and Auto Loader (schema enforcement at ingestion), respectively -
platform features this library's results table and
`raise_on_critical_failures()` are designed to plug into, not duplicate.
Likewise, CI/CD change-management gating is an orchestration/pipeline
concern - the `tests/` suite here is the regression asset such a gate would
run, not something this library runs itself.

---

## Full `CheckDefinition` field reference

Every field on the dataclass. A check type only needs the ones listed for
it above; unused fields are just left `None`.

| Field | Type | Used by |
|---|---|---|
| `name` | str (required) | all - must be unique within a config |
| `type` | str (required) | all - one of the 18 types above |
| `severity` | `"critical"` \| `"warning"` (default `"warning"`) | all |
| `threshold` | float 0.0-1.0 (default `1.0`) | all |
| `column` | str | completeness, sentinel_value, coverage, period_gap, uniqueness, range, value_set, regex, referential_integrity, accuracy, reconciliation, cross_dataset_consistency, anomaly |
| `columns` | list[str] | uniqueness (composite key), cross_dataset_consistency (group-by), immutability (fingerprint columns) |
| `treat_blank_as_null` | bool (default `false`) | completeness |
| `disallowed_values` | list | sentinel_value |
| `frequency` | `"day"` \| `"month"` \| `"year"` (default `"day"`) | period_gap |
| `min` / `max` | float | range, row_count |
| `allowed_values` | list | value_set |
| `pattern` | str | regex |
| `expression` | str | expression |
| `ref_dataset` | str | referential_integrity, accuracy, reconciliation, cross_dataset_consistency, coverage |
| `ref_column` | str | referential_integrity (required), coverage (required), accuracy (optional, defaults to `column`) |
| `match_column` / `ref_match_column` | str | accuracy (`ref_match_column` optional, defaults to `match_column`) |
| `max_age_hours` | float | freshness |
| `expected_schema` | dict[str, str] | schema |
| `tolerance` | float ≥ 0 (default `0.0`) | cross_dataset_consistency |
| `max_pct_change` | float ≥ 0 (default `0.5`) | anomaly |
| `lookback` | int ≥ 1 (default `5`) | anomaly |
| `filter_expression` | str | any check type |

---

## Writing and loading configs

One YAML file per dataset:

```yaml
dataset: customers    # logical dataset name, recorded in every result row
layer: silver           # bronze/silver/gold - pure metadata, never changes check behavior

checks:
  - name: customer_id_not_null
    type: completeness
    column: customer_id
    severity: critical
    threshold: 1.0
  # ... more checks
```

```python
from pyspark_dq import load_config, load_configs

config = load_config("configs/example_silver_customers.yaml")   # one file -> one DatasetConfig
configs = load_configs("configs/")                                 # a directory -> list[DatasetConfig], one per *.yaml/*.yml file
```

See `configs/example_bronze_orders.yaml`, `configs/example_silver_customers.yaml`,
and `configs/example_gold_revenue_summary.yaml` for three complete, working
configs - one per medallion layer, each exercising several check types
together.

---

## Running it

### As a library, from a notebook

```python
import sys
sys.dont_write_bytecode = True

PKG_ROOT = "/Workspace/Users/mkh.mukherjee@gmail.com/pyspark-dataquality"
if PKG_ROOT not in sys.path:
    sys.path.insert(0, PKG_ROOT)

from pyspark_dq import DQEngine, load_config

config = load_config(f"{PKG_ROOT}/configs/example_silver_customers.yaml")
df = spark.table("mycatalog.silver.customers")
countries_df = spark.table("mycatalog.silver.countries")

engine = DQEngine(spark)
history_df = spark.table("mycatalog.dq.dq_results")   # only needed if the config has anomaly/immutability checks
result_df = engine.run(df, config, ref_dfs={"countries": countries_df}, history_df=history_df)
display(result_df)

engine.write_results(result_df, "mycatalog.dq.dq_results")
engine.raise_on_critical_failures(result_df)
```

### As a Databricks Job

**Option A - notebook task.** Create a notebook that does the setup above
plus your `engine.run(...)` calls, then point a Databricks Workflow's
notebook task at it. Simplest option; works on serverless or classic
compute.

**Option B - `spark_python_task` calling the CLI directly.**
`pyspark_dq/cli.py` is a standalone entry point built for exactly this - one
job task per run, no notebook needed:

```
python_file: /Workspace/Users/mkh.mukherjee@gmail.com/pyspark-dataquality/pyspark_dq/cli.py
parameters:
  --config /Workspace/Users/mkh.mukherjee@gmail.com/pyspark-dataquality/configs/customers_silver.yaml
  --input mycatalog.silver.customers
  --ref-table countries=mycatalog.silver.countries
  --results-table mycatalog.dq.dq_results
```

#### CLI flag reference

| Flag | Required | Meaning |
|---|---|---|
| `--config` | yes | A YAML config file, or a directory of them (runs every dataset's checks in one job) |
| `--input` | yes | Table name (`catalog.schema.table`) or storage path for the dataset under test |
| `--input-format` | no (default `delta`) | Format to use when `--input` is a path |
| `--ref-table NAME=LOCATION` | no, repeatable | A reference dataset, for `referential_integrity`/`accuracy`/`reconciliation`/`cross_dataset_consistency` checks |
| `--results-table` | yes | Table name or path to append results to. Also auto-read as history for `anomaly`/`immutability` checks, before this run's rows are written |
| `--results-format` | no (default `delta`) | Format for `--results-table` |
| `--no-fail-on-critical` | no (flag) | Record results but don't raise/exit non-zero on a critical failure |

Exit code is `1` if any critical check failed (unless `--no-fail-on-critical`
is set), `0` otherwise - wire that straight into a Workflow task's
success/failure.

---

## The results table

One row per check per run, appended (never overwritten) by
`write_results()`, so it accumulates into a full audit history across every
dataset/layer/run:

| Column | Type | Notes |
|---|---|---|
| `run_id` | string | one UUID per `run()` call |
| `run_timestamp` | timestamp | UTC |
| `dataset`, `layer` | string | from the `DatasetConfig` |
| `check_name`, `check_type`, `severity` | string | from the `CheckDefinition` |
| `column` | string, nullable | the primary column the check was about, where applicable |
| `status` | string | `"PASS"` or `"FAIL"` |
| `threshold` | double | the configured threshold |
| `pass_rate` | double, nullable | fraction that passed, where the check type produces one |
| `total_rows`, `failed_rows` | long, nullable | row-level counts, where applicable |
| `message` | string | human-readable detail |
| `metric_value` | double, nullable | the raw number a check computed (`reconciliation`'s actual count, `anomaly`'s metric) |
| `metric_text` | string, nullable | non-numeric payload (`immutability`'s checksum) |

Build a dashboard straight off this table (pass-rate trend by
dataset/check_type over time, current FAIL count by severity, etc.) - it's
the same "DQ metrics store" concept the org's Data Quality Framework doc
calls for in Section 5.6/8.2, minus the alert-routing wiring (that part is
a Databricks Lakehouse Monitoring / Workflow concern, not this library's).

---

## Extending the framework

`pyspark_dq/checks/` is a real Python package, one module per check type,
specifically so it's importable at whatever granularity you need:

```python
from pyspark_dq import DQEngine, load_config       # the normal path
from pyspark_dq.checks import completeness           # or: just one check's logic, nothing else pulled in
```

`engine.py` has no per-check-type logic of its own - it dispatches through
`checks.ROW_LEVEL_CHECKS` / `checks.DATASET_LEVEL_CHECKS`, two dicts
assembled in `checks/__init__.py`. Row-level modules (batched into one
aggregation per dataset by the engine) expose:

```python
def build_predicate(check: CheckDefinition, df: DataFrame) -> tuple[DataFrame, Column]: ...
```

Dataset-level modules (each evaluated independently) expose:

```python
def evaluate(check, df, *, ref_df=None, history_df=None, dataset_name=None) -> CheckOutcome: ...
```

Adding a new check type is: write a module matching one of those two
shapes, add it to `VALID_CHECK_TYPES` in `models.py`, add one line to the
registry dict in `checks/__init__.py`. `engine.py` never needs to change.

---

## Running the tests

```python
import sys
sys.dont_write_bytecode = True

PKG_ROOT = "/Workspace/Users/mkh.mukherjee@gmail.com/pyspark-dataquality"
if PKG_ROOT not in sys.path:
    sys.path.insert(0, PKG_ROOT)
```
```
%pip install pytest pyyaml
```
```python
import pytest
pytest.main(["-v", f"{PKG_ROOT}/tests/test_checks.py"])
```

This has been run end-to-end on this workspace's serverless compute (20/20
passing, covering every check type added on top of the original nine plus
`filter_expression` and `treat_blank_as_null`). The `spark` fixture in
`tests/conftest.py` detects when it's running inside
a Databricks notebook/job (`DATABRICKS_RUNTIME_VERSION` or `SPARK_REMOTE` in
the environment) and reuses the ambient Spark session instead of trying to
start a local one - serverless compute only speaks Spark Connect and rejects
a second, local-mode session outright.

---

## Known Workspace Files gotchas

Both of these are mount limitations of Workspace Files, not bugs in this
package - included here so they don't get rediscovered:

1. **No `__pycache__` writes.** Importing anything from a package that lives
   under `/Workspace/...` fails with `OSError: [Errno 95] Operation not
   supported` unless `sys.dont_write_bytecode = True` is set *before* the
   import.
2. **pytest's config-file auto-discovery can misbehave on some mounted
   paths** (observed on a Unity Catalog Volume, not on Workspace Files) -
   if you ever see `OSError: [Errno 22] Invalid argument` while pytest is
   searching for `pytest.ini`/`pyproject.toml`, pass `-c <path-to-a-real-
   pytest.ini>` explicitly to stop the upward directory search.

---

## Quick-reference table

| type | dimension | what it checks | needs `ref_dfs` | needs `history_df` |
|---|---|---|:---:|:---:|
| `completeness` | Completeness | column is non-null (optionally: or blank) | | |
| `sentinel_value` | Completeness | column isn't a disguised-null placeholder | | |
| `coverage` | Completeness | every reference key appears in this dataset | ✅ | |
| `period_gap` | Completeness | no missing day/month/year in a date range | | |
| `row_count` | Completeness | row count within bounds | | |
| `uniqueness` | Uniqueness | no duplicate values | | |
| `range` | Validity | numeric value within min/max | | |
| `value_set` | Validity | value in an allow-list | | |
| `regex` | Validity | value matches a pattern | | |
| `schema` | Validity | dtypes match expected | | |
| `expression` | Validity | arbitrary SQL predicate, per row | | |
| `referential_integrity` | Accuracy | key exists in reference dataset | ✅ | |
| `accuracy` | Accuracy | mapped attribute matches reference | ✅ | |
| `reconciliation` | Consistency | count/sum matches reference dataset | ✅ | |
| `cross_dataset_consistency` | Consistency | per-group sum matches reference dataset | ✅ | |
| `immutability` | Consistency | historical data unchanged since last run | | ✅ |
| `freshness` | Timeliness | latest timestamp isn't stale | | |
| `anomaly` | Timeliness / Volume | metric isn't a big swing vs. history | | ✅ |
