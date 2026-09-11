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
| `ref_dfs` | A `{name: DataFrame}` dict passed to `run()`, for checks that compare against a second dataset (`referential_integrity`, `accuracy`, `reconciliation`, `cross_dataset_consistency`, `coverage`, `cross_source_duplicate`). |
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
  trim_whitespace: true      # compare trimmed values - without this, " C1001" and "C1001" look like different keys
  severity: critical
  threshold: 1.0
```
Catches: "exact duplicate records" and "primary key violation" - the same
outlet/customer/order appearing twice - and, via `columns`, "composite key
duplication" (uniqueness violated only when checked across several columns
together, e.g. `columns: [store_id, sku, sale_date]`). Rows with a null key
are excluded (that's `completeness`'s job) so they never count as
duplicates. Set `trim_whitespace: true` to also catch "whitespace/trailing
space issues" - `" John "` and `"John"` otherwise compare as distinct keys,
silently hiding a real duplicate from detection.

Run against the "one" side of a join *before* that join happens, this is
also the root-cause fix for "fan-out/fan-in errors in joins": unintended
row multiplication (or loss) from an unexpectedly non-unique join key. A
`uniqueness` check on `customers.customer_id` prevents the surprise;
`reconciliation` (Consistency, below) on the joined output's row count
catches it after the fact if it happens anyway - the two are complementary,
not alternatives.

**`fuzzy_duplicate`** - flags rows whose `column` is a near-match (edit
distance ≤ `max_edit_distance`, default 2) of another row's, not just an
exact one. Pass `columns` as a blocking key to scope the comparison instead
of a full cross join.

```yaml
- name: no_near_duplicate_customer_names
  type: fuzzy_duplicate
  column: full_name
  columns: [postal_code]   # blocking key - only compare rows that already share a postal code
  max_edit_distance: 2
  severity: warning
  threshold: 0.98
```
Catches: "Jon Smith" vs. "John Smith" - the same real-world entity recorded
under two different spellings. `uniqueness` only catches an *exact* repeat;
this catches a near one. Without a blocking key every row is compared
against every other row (O(n²)) - only skip `columns` on a dataset small
enough, or already scoped via `filter_expression`, for that to be fine.

**`cross_source_duplicate`** - flags keys in `column` that also appear in a
reference dataset's `ref_column` - records about to collide if the two were
merged/unioned as-is.

```yaml
- name: backfill_does_not_reinsert_already_loaded_orders
  type: cross_source_duplicate
  column: order_id
  ref_dataset: orders_already_loaded   # must be a key in the ref_dfs dict / --ref-table
  ref_column: order_id
  severity: critical
  threshold: 1.0
```
Catches two scenarios that both reduce to "do these two key-sets overlap":
"duplicate across merge/union sources" (`ref_dataset` is another source
system about to be unioned in) and "duplicate due to late-arriving data
reprocessing" (`ref_dataset` is the target table a backfill is about to
write into). `uniqueness` can't catch either - it only checks for
duplicates *within* one already-merged dataset, not whether two *separate*
datasets are about to collide. (For "duplicate due to reprocessing" within
a single idempotent re-run, see `reconciliation`, above.)

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
Catches "domain/range violations" like a discount percentage of 150%.

**`length`** - a column's non-null values must have a size in `[min, max]`.
The size analog of `range`: character length for a string, element count
for an array or map column (auto-detected from the column's actual type).

```yaml
- name: order_id_length_is_valid
  type: length
  column: order_id
  min: 8
  max: 12
  severity: warning
  threshold: 0.99
```
Catches "field exceeds/undercuts expected character length" - a code that's
too short or too long to be a real value for this field - and, pointed at
an `array`/`map` column instead of a string one, "inconsistent array
lengths" in a semi-structured feed (e.g. a malformed JSON payload producing
a wildly oversized array).

**`castable`** - a column's non-null values must actually convert to
`target_type` (a Spark SQL type name), using `try_cast` rather than a
format-only pattern.

```yaml
- name: raw_date_is_a_real_date
  type: castable
  column: created_at_raw
  target_type: timestamp
  severity: critical
  threshold: 0.99
```
Catches "format violation" and "data type" issues - "13/45/2023" or a
numeric field containing alphabetic characters - and does it more reliably
than a hand-written regex: `try_cast(..., "date")` correctly rejects
"2023-02-30" (shaped like a date, but not a real one) the way a
format-only pattern can't.

**`value_set`** - a column's non-null values must be in an allow-list.
Exact match, case-sensitive by default.

```yaml
- name: status_is_known_value
  type: value_set
  column: status
  allowed_values: [ACTIVE, INACTIVE, PENDING]
  severity: warning
  threshold: 1.0
```
Catches an unrecognized/misspelled enum value ("Pnding" instead of
"Pending") and - because the match is case-sensitive - a "case sensitivity
violation" too (an enum stored as "Pending" when only "PENDING" is
allowed fails this exactly as it should). Also covers "inconsistent naming
conventions" ("NY" vs. "N.Y." vs. "New York" for the same value) and
"non-standardized units of measure" ("kg" vs. "Kg" vs. "kilograms") by
pinning the one canonical spelling as the only `allowed_values` entry - if
you don't know the canonical form upfront and just want to detect a split,
use `uniform_value` (Consistency, below) instead. Paired with `completeness`
on a grain-discriminator column (e.g. `period_type: DAILY|MONTHLY`), this
also catches "inconsistent grain" - a table silently mixing daily and
monthly rows - by making sure every row is explicitly tagged with a valid
grain instead of leaving it to guesswork.

**`regex`** - a column's non-null values must match a pattern.

```yaml
- name: email_looks_valid
  type: regex
  column: email
  pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
  severity: warning
  threshold: 0.98
```
Catches "pattern/regex violations" (a missing `@`, a malformed phone
number) and, with a pattern restricted to expected character ranges (e.g.
`^[\x20-\x7E]*$` for printable ASCII), "encoding violations" - non-UTF8
bytes producing garbled/mojibake text - and "special character handling"
issues, e.g. a pattern like `'^[^\t\n\r"]*$'` to reject unescaped tabs,
newlines, or quotes within a field.

**`format_consistency`** - `column`'s non-null values should all use the
same one of several possible representations, without needing to know
upfront which one is "correct." `format_patterns` is a list of regex
patterns for the distinct acceptable formats; the majority format among
recognized values becomes the baseline.

```yaml
- name: created_at_raw_format_is_consistent
  type: format_consistency
  column: created_at_raw
  format_patterns:
    - '^\d{4}-\d{2}-\d{2}'    # ISO: YYYY-MM-DD...
    - '^\d{2}/\d{2}/\d{4}$'     # US: MM/DD/YYYY
  severity: warning
  threshold: 0.98
```
Catches "inconsistent date/time formats" - "MM/DD/YYYY" and "DD-MM-YYYY"
mixed in the same field - and, pointed at a numeric-looking string column
with patterns for `"1,234.56"` vs. `"1.234,56"`, "locale-specific
formatting issues" (decimal comma vs. decimal point confusion). Different
from `regex`: `regex` validates against one already-known-correct pattern;
this is for when there should be exactly one format, but you don't know in
advance which one.

**`schema`** - the DataFrame's actual dtypes must match an expected map.
Metadata-only (`df.dtypes`, no Spark job triggered), so this is essentially
free. Spark's dtype strings are fully recursive (a struct/array column's
dtype spells out its whole nested shape), so this already catches nested
schema drift too, not just flat top-level columns.

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
  strict: true          # also fail on a column that isn't listed here
  enforce_order: true     # also fail if these columns appear out of order
```
Catches: "schema drift," "data type drift," and "metadata schema mismatch"
- a source column silently renamed, retyped, or dropped, or a data catalog
schema that no longer matches the actual table - which doubles as a
"versioning issues" alarm: a schema check that starts failing *is* the
detection that the schema changed without whatever version control was
supposed to be tracking it. Set `strict: true` to also catch "unexpected/
extra columns" - a new, unmapped column quietly showing up in the feed -
which isn't a failure by default (an unlisted column existing isn't
inherently wrong). Set `enforce_order: true` to also catch "column order
dependency failure" - a pipeline that assumed a fixed column position
breaking when the source reorders them; most pipelines bind by name and
genuinely don't care, so this isn't checked by default either.

**`expression`** - any SQL boolean predicate, evaluated per row. The
open-ended escape hatch for business rules that don't fit another type.

```yaml
- name: current_month_data_only
  type: expression
  expression: "month(report_date) = month(current_date()) AND year(report_date) = year(current_date())"
  severity: critical
  threshold: 1.0
```
Catches: e.g. a market-share report accidentally including last month's
rows, or a same-row ordering rule like `ship_date >= order_date` -
"cross-field inconsistency" where two columns on the same row logically
contradict each other.

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
Catches: a country_code with no corresponding master record - "referential/
lookup code invalid" (e.g. a country code of "ZZ" that isn't in the ISO
list), "orphan records" (a foreign key referencing a non-existent parent),
and "broken referential integrity after deletes" (the parent row was
removed, leaving a dangling reference) - all the same mechanism, whichever
way the orphan came to exist. `ref_dfs` doesn't care what catalog or
database a DataFrame was read from, so this also covers "cross-database
referential mismatch" without any special handling.

**`no_circular_reference`** - following `parent_column` from any row in
`column` must never lead back to that same row, within `max_depth` hops
(default 20). Spark has no native recursive query support, so this is a
bounded iterative self-join - each hop is one more join, so don't raise
`max_depth` past what the hierarchy could plausibly need.

```yaml
- name: no_org_hierarchy_loop
  type: no_circular_reference
  column: employee_id
  parent_column: manager_id
  max_depth: 10
  severity: critical
  threshold: 1.0
```
Catches: "circular references" (A references B, B references A) and
"hierarchy/parent-child violations" - including the simplest case, an
employee who reports to themselves, which is just a cycle of length one and
needs no special-casing.

For "cardinality violation" (a 1:1 relationship secretly has multiple
matches), combine `uniqueness` on the *child* table's foreign key (rules
out 1:many) with `coverage` or `referential_integrity` (rules out 1:zero) -
no new check type needed, that combination already proves exactly 1:1. For
"SCD integrity issues" (multiple rows flagged `current = Y` for the same
key), scope a plain `uniqueness` check with `filter_expression: "is_current
= 'Y'"` - the same composability `scd_overlap`'s date-range version doesn't
need for this simpler boolean-flag pattern.

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
just linked to the wrong state. Also covers "wrong data source mapped" and
"incorrect classification" - anywhere a value was populated from the wrong
place - and, pointed at an FX-rate reference table, "using a stale exchange
rate for a currency conversion."

Set `value_map` when the two systems use different code schemes for the
same fact:

```yaml
- name: is_active_matches_crm
  type: accuracy
  column: customer_id
  match_column: status            # this table stores ACTIVE/INACTIVE
  ref_dataset: crm
  ref_match_column: active_flag    # the CRM stores 1/0 for the same fact
  value_map: {ACTIVE: 1, INACTIVE: 0, PENDING: 0}
  severity: critical
  threshold: 1.0
```
Catches: "customer status = Active in CRM but Closed in Billing" where the
two systems don't even agree on how to *spell* active/closed - `value_map`
translates this side's raw value before comparing, so "ACTIVE" vs. `1`
isn't a false mismatch. A raw value with no entry in `value_map` still
counts as a mismatch (an unrecognized code is itself worth flagging, not
something to silently skip).

Set `abs_tolerance` instead when the two systems' values should agree only
*approximately* - most commonly two clocks:

```yaml
- name: updated_at_matches_crm_clock
  type: accuracy
  column: customer_id
  match_column: updated_at             # this system's clock
  ref_dataset: crm
  ref_match_column: last_modified_at    # the CRM's clock for the same event
  abs_tolerance: 300                     # allow up to 5 minutes of drift
  severity: warning
  threshold: 0.98
```
Catches: "clock/timezone drift" between two systems - two clocks will never
agree to the millisecond, so this allows a tolerance instead of requiring
an exact match. Both sides are cast to `double` before differencing, which
turns a timestamp into Unix epoch seconds automatically - so
`abs_tolerance` is in seconds for a timestamp `match_column`, or in the
column's own units for a plain numeric one.

**`stale_record`** - `column`'s own value must be within `max_age_hours` of
now, evaluated per row - not just the dataset's most recent value.

```yaml
- name: customer_address_not_stale
  type: stale_record
  column: updated_at
  max_age_hours: 8760   # ~1 year
  severity: warning
  threshold: 0.95         # allow up to 5% of customers to be overdue for a refresh
```
Catches: "address not updated after customer moved" - a specific record
that's gone stale while the rest of the table updates normally.
`freshness` (Timeliness, below) only proves the *newest* row in the table
is recent; it says nothing about an individual row nobody's touched.

**`derived_field`** - a column's non-null values must equal `expression` (a
formula over other columns), within `abs_tolerance` (default 0 = exact).

```yaml
- name: amount_matches_quantity_times_unit_price
  type: derived_field
  column: amount
  expression: "quantity * unit_price"
  abs_tolerance: 0.01     # allow a cent of rounding drift
  severity: warning
  threshold: 0.99
```
Catches: "total != sum of line items due to a formula error," or a currency
amount truncated instead of rounded. (For a formula that spans two
datasets - e.g. an order header's total vs. the sum of its own order
lines - group both by the order key and use `cross_dataset_consistency`
instead, below.) Also the fix for "undocumented assumptions in derived
fields" - a calculated KPI whose logic wasn't traceable to any rule: once
the formula is a `derived_field` check, the config *is* the rule,
versioned and enforced instead of buried in a notebook.

**`outlier`** - a column's non-null values must fall within `num_std_dev`
(default 3) standard deviations of the dataset's own mean.

```yaml
- name: sensor_reading_not_an_outlier
  type: outlier
  column: reading
  num_std_dev: 3
  severity: warning
  threshold: 0.999
```
Catches: a faulty IoT sensor or a fat-fingered manual entry - one row's
value that's statistically implausible next to all the others *in this same
run*. This is a different mechanism from `anomaly` (Timeliness, below),
which compares one aggregate number *across runs*, not individual rows
within one.

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
pipeline - "row count mismatch" (source has 10,000 rows, target loaded only
9,850), "duplicate due to reprocessing" (a pipeline re-run without
idempotency double-counting rows) when `ref_dataset` points at an "already
loaded batches" marker, and "real-time vs. batch latency mismatch" causing
reconciliation gaps when `ref_dataset` points at the other layer (streaming
vs. batch). With `column` set, the same check becomes a "control total
mismatch" check - the sum of a financial column in the source should equal
the sum in the target, not just the row count. Chained across layers (bronze
vs. silver, silver vs. gold, one `reconciliation` check per hop) this is
also the direct fix for "reconciliation break between layers" not tying
out, and pointed at a pre- vs. post-transformation dataset, for "data loss
during transformation" (a filter/dedup step unintentionally dropping valid
rows) or "data loss during transmission" (a truncated file transfer).

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
downstream aggregate silently drifted from the source. Grouping by a period
column (e.g. `columns: [month]`) also catches "monthly total != sum of
daily totals due to differing aggregation rules" - group the daily table by
month and compare against the monthly table's own totals.

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
Applied to a reference/lookup table, it's also a "versioning issues" alarm
for data that isn't under formal SCD Type 2 control (see `scd_overlap`,
above, for tables that are): any unexpected change gets flagged even
without a version history to check against.

**`scd_overlap`** - no two rows for the same entity (`columns`) may have
overlapping `[start_column, end_column)` validity windows. A `NULL`
`end_column` means "still the current version."

```yaml
- name: no_customer_version_overlap
  type: scd_overlap
  columns: [customer_id]     # entity key
  start_column: valid_from
  end_column: valid_to
  severity: critical
  threshold: 1.0
```
Catches: the classic SCD Type 2 defect - "same entity has conflicting
states at the same timestamp" because a merge produced two concurrently-
"current" versions. `uniqueness` can't see this: it only catches two rows
sharing the exact same key, not two rows whose *date ranges* overlap
without ever sharing one identical value.

**`uniform_value`** - a column's non-null values must all be the same - no
silently mixed units/currencies/codes within one dataset.

```yaml
- name: currency_is_uniform
  type: uniform_value
  column: currency
  allowed_values: [USD]   # pin the expected value explicitly - omit to just detect a split via majority vote
  severity: critical
  threshold: 1.0
```
Catches: "same metric reported in different units within the same
dataset" - a currency column quietly mixing USD and EUR, or a weight column
mixing kg and lbs. Unlike `value_set`, no pre-known allow-list is required:
omit `allowed_values` and the majority value becomes the baseline, so a
split still gets flagged even without knowing in advance which value is
"correct."

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
Catches: "stale data" and "delayed batch jobs" - a scheduled job that ran
but didn't actually pick up new data (stale source), *if* this check itself
is run on a schedule independent of whether the upstream job succeeded -
see the note in ["What's still out of scope, on
purpose"](#whats-still-out-of-scope-on-purpose). Also covers "missed
SLA/refresh window" (set `max_age_hours` to the contractual refresh
frequency) and "stale metadata" (a catalog says "daily refresh" but the job
now runs weekly) - set `max_age_hours` to what the catalog *declares*, and
a failure here is exactly the gap between the declared metadata and reality;
for "late-arriving data past an SLA cutoff," pair `row_count` (`min: 1`)
with `filter_expression` scoped to today's expected arrival window instead,
with the same orchestration-timing caveat.

**`monotonicity`** - `column` must never decrease when rows are ordered by
`order_by`. Optionally scoped to `columns` (checked separately within each
partition).

```yaml
- name: events_arrive_in_order
  type: monotonicity
  column: event_time      # must never decrease...
  order_by: arrival_seq     # ...as arrival_seq increases
  columns: [device_id]        # check ordering per device, not across the whole feed
  severity: warning
  threshold: 0.98
```
Catches: "out-of-order events" in stream processing - an event whose
timestamp is earlier than the previous one for the same entity, despite
arriving later. Uses a window `LAG` (the same pattern `uniqueness` uses) to
compare each row against its predecessor in `order_by` order; without a
`columns` partition key this forces a single-partition sort, so scope it on
anything large.

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
that doubled overnight, and "sudden volume spike/drop" - a daily load that
deviates significantly from the historical pattern, whether or not it also
fails a `reconciliation` check against a specific reference count. Passes
automatically until enough run history has accumulated - a brand-new
dataset never false-fails on day one.

Set `metric` to track something other than sum/count:

```yaml
- name: revenue_null_rate_is_stable
  type: anomaly
  column: total_revenue
  metric: null_rate      # or "distinct_count"; defaults to "sum" (column set) or "count" (column omitted)
  max_pct_change: 5.0
  severity: warning
```
`metric: null_rate` (fraction of `column` that's null) catches "anomalous
null rate spike" - a field's null percentage jumping from 1% to 40%.
`metric: distinct_count` catches "cardinality anomaly" - a categorical
column's unique-value count changing abruptly.

Set `seasonal_period` (`"month"`, `"day_of_week"`, or `"day_of_month"`) to
compare against the same calendar period in history instead of just the
most recent runs:

```yaml
- name: revenue_matches_seasonal_pattern
  type: anomaly
  column: total_revenue
  seasonal_period: month   # compare against the same month in prior years
  lookback: 3                # average of the last 3 same-month runs
  max_pct_change: 0.3
  severity: warning
```
Catches "seasonal pattern break" - an expected holiday spike that didn't
happen. Without `seasonal_period`, the baseline is the average of the last
`lookback` runs regardless of when they fell, which would just see "normal
for last month" and miss a break in a pattern that only recurs annually.

**`distribution_shift`** - the proportion of each value in a categorical
`column` should stay close to the distribution recorded for this check in
the last run. `max_pct_change` (reused from `anomaly`, default 0.1) bounds
the largest allowed shift in any one category's share.

```yaml
- name: region_mix_is_stable
  type: distribution_shift
  column: region
  max_pct_change: 0.1
  severity: warning
```
Catches "unexpected distribution shift (data drift)" - category proportions
moving drastically vs. the historical baseline - and "skewed/imbalanced
data" - one category unexpectedly coming to dominate 99% of records. Unlike
`anomaly`, which tracks one number across runs, this compares an entire
category mix; it stores today's distribution as JSON in `metric_text` so
the next run has something to compare against, and passes automatically
until there's a prior distribution recorded.

**`correlation_shift`** - the Pearson correlation between `column` and
`match_column` should stay close to the average correlation recorded for
this check over the last `lookback` runs. Uses `abs_tolerance` (default
0.2) rather than a percentage change, since correlation is bounded to
[-1, 1] and can cross zero, where a percentage change is meaningless.

```yaml
- name: revenue_still_tracks_order_volume
  type: correlation_shift
  column: total_revenue
  match_column: order_count
  abs_tolerance: 0.3
  lookback: 7
  severity: warning
```
Catches "correlation break" - two historically correlated metrics suddenly
diverging - and, pointed at a feature/target pair in an ML-consuming
pipeline, "concept drift" (the relationship between them changing over
time). Passes automatically until there's a baseline to compare against,
same as `anomaly`.

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
`cross_dataset_consistency`, `coverage`, and `cross_source_duplicate` all
read `check.ref_dataset` as a key into a `ref_dfs` dict you pass to `run()`:

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

A few things the gap-analysis passes flagged are *not* implemented here -
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

Metadata and governance concerns split the same way: "broken/missing
lineage" (tracing a field back to its source system) and
"ownership/stewardship gaps" (no identified data owner) are Unity Catalog's
job - lineage graphs and ownership tags, not data values, and this library
never touches either. "Missing/incorrect data dictionary" and "undocumented
transformation logic" are documentation-practice problems - no check over a
DataFrame's *values* can verify that a column's *documented meaning*
matches its content, or that the code that produced it has comments. What
this library *can* do, and does: `schema` makes the structural half of a
data dictionary (names, types) enforceable as code, `derived_field` makes a
KPI's calculation formula explicit and traceable instead of buried in a
notebook, and `expression` enforces one agreed business definition (e.g.
"active customer") consistently once a team has actually agreed on it -
resolving the disagreement itself is still a governance conversation, not
something a check can settle.

---

## Full `CheckDefinition` field reference

Every field on the dataclass. A check type only needs the ones listed for
it above; unused fields are just left `None`.

| Field | Type | Used by |
|---|---|---|
| `name` | str (required) | all - must be unique within a config |
| `type` | str (required) | all - one of the 32 types above |
| `severity` | `"critical"` \| `"warning"` (default `"warning"`) | all |
| `threshold` | float 0.0-1.0 (default `1.0`) | all |
| `column` | str | completeness, sentinel_value, coverage, period_gap, stale_record, derived_field, outlier, uniform_value, castable, length, fuzzy_duplicate, cross_source_duplicate, monotonicity, no_circular_reference, format_consistency, distribution_shift, correlation_shift, uniqueness, range, value_set, regex, referential_integrity, accuracy, reconciliation, cross_dataset_consistency, anomaly |
| `metric` | `"sum"` \| `"count"` \| `"null_rate"` \| `"distinct_count"` | anomaly (default: `sum` if `column` set, else `count`) |
| `seasonal_period` | `"month"` \| `"day_of_week"` \| `"day_of_month"` | anomaly (optional - baseline is calendar-aligned instead of just "the last N runs") |
| `columns` | list[str] | uniqueness (composite key), cross_dataset_consistency (group-by), immutability (fingerprint columns), scd_overlap (entity key), fuzzy_duplicate (optional blocking key), monotonicity (optional partition key) |
| `order_by` | str | monotonicity (required - the column defining arrival order) |
| `parent_column` | str | no_circular_reference (required) |
| `max_depth` | int ≥ 1 (default `20`) | no_circular_reference |
| `max_edit_distance` | int ≥ 0 (default `2`) | fuzzy_duplicate |
| `format_patterns` | list[str] | format_consistency (required) |
| `trim_whitespace` | bool (default `false`) | uniqueness |
| `treat_blank_as_null` | bool (default `false`) | completeness |
| `disallowed_values` | list | sentinel_value |
| `frequency` | `"day"` \| `"month"` \| `"year"` (default `"day"`) | period_gap |
| `abs_tolerance` | float ≥ 0 (default `0.0`) | derived_field, accuracy (optional - switches from exact match to a tolerance comparison), correlation_shift (default `0.2`) |
| `num_std_dev` | float > 0 (default `3.0`) | outlier |
| `start_column` / `end_column` | str | scd_overlap (both required; `end_column` may be `NULL` per row = "still current") |
| `value_map` | dict | accuracy (translates this side's raw value before comparing) |
| `target_type` | str | castable (a Spark SQL type name, e.g. `date`, `double`, `timestamp`) |
| `min` / `max` | float | range, row_count, length (character count for a string column, element count for an array/map column) |
| `allowed_values` | list | value_set, uniform_value (optional - a single value to pin the expected baseline) |
| `pattern` | str | regex |
| `expression` | str | expression, derived_field (the formula `column` should equal) |
| `ref_dataset` | str | referential_integrity, accuracy, reconciliation, cross_dataset_consistency, coverage, cross_source_duplicate |
| `ref_column` | str | referential_integrity (required), coverage (required), cross_source_duplicate (required), accuracy (optional, defaults to `column`) |
| `match_column` / `ref_match_column` | str | accuracy (`ref_match_column` optional, defaults to `match_column`); `match_column` also required by correlation_shift (the other column to correlate against) |
| `max_age_hours` | float | freshness, stale_record |
| `expected_schema` | dict[str, str] | schema |
| `strict` | bool (default `false`) | schema (also fail on a column not listed in `expected_schema`) |
| `enforce_order` | bool (default `false`) | schema (also fail if the expected columns appear out of order) |
| `tolerance` | float ≥ 0 (default `0.0`) | cross_dataset_consistency |
| `max_pct_change` | float ≥ 0 (default `0.5`) | anomaly, distribution_shift (default `0.1`) |
| `lookback` | int ≥ 1 (default `5`) | anomaly, correlation_shift |
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

This has been run end-to-end on this workspace's serverless compute (71/71
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
| `fuzzy_duplicate` | Uniqueness | no near-duplicate values (edit distance) | | |
| `cross_source_duplicate` | Uniqueness | keys don't overlap with a reference dataset | ✅ | |
| `range` | Validity | numeric value within min/max | | |
| `length` | Validity | string/array/map size within min/max | | |
| `castable` | Validity | value actually converts to a target type | | |
| `value_set` | Validity | value in an allow-list | | |
| `regex` | Validity | value matches a pattern | | |
| `format_consistency` | Validity | column isn't silently mixing formats | | |
| `schema` | Validity | dtypes (and optionally column set/order) match expected | | |
| `expression` | Validity | arbitrary SQL predicate, per row | | |
| `referential_integrity` | Accuracy | key exists in reference dataset | ✅ | |
| `no_circular_reference` | Accuracy | self-referential hierarchy has no cycle | | |
| `accuracy` | Accuracy | mapped attribute matches reference | ✅ | |
| `stale_record` | Accuracy | this row's own timestamp isn't stale | | |
| `derived_field` | Accuracy | column equals a formula, within tolerance | | |
| `outlier` | Accuracy | value isn't a statistical outlier vs. this run | | |
| `reconciliation` | Consistency | count/sum matches reference dataset | ✅ | |
| `cross_dataset_consistency` | Consistency | per-group sum matches reference dataset | ✅ | |
| `immutability` | Consistency | historical data unchanged since last run | | ✅ |
| `scd_overlap` | Consistency | no two entity versions have overlapping date ranges | | |
| `uniform_value` | Consistency | column doesn't silently mix values (units/codes) | | |
| `freshness` | Timeliness | latest timestamp isn't stale | | |
| `monotonicity` | Timeliness | value never decreases in arrival order | | |
| `anomaly` | Timeliness / Volume | metric isn't a big swing vs. history | | ✅ |
| `distribution_shift` | Timeliness / Volume | category mix isn't a big swing vs. history | | ✅ |
| `correlation_shift` | Timeliness / Volume | two columns' correlation isn't a big swing vs. history | | ✅ |
