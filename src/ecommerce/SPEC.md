# E-commerce CDC Streaming Lab: Test Scenarios

Nov 24, 2026 · Nishesh

> Source: [Claude Docs](https://claude.ai/artifact/Nw5aVjW3E1nx8Y6QSd28bY). Keep this file in sync when the doc changes.

## Overview

One simulated e-commerce source, landing as JSON files in a Unity Catalog Volume, drives every test in this catalog. Auto Loader stands in for Kafka, and the files use the Debezium CDC envelope, so the pipeline code is the same shape you would run against a real feed.

**Layout (catalog `ecom_lab`)**

- `ecom_lab.landing` volume: `/cdc/customers`, `/cdc/products`, `/cdc/inventory`, `/cdc/orders`, `/events/clicks`
- `ecom_lab.bronze`: raw append-only streaming tables, one per feed, plus `_ingest_ts` and `_source_file`
- `ecom_lab.silver`: cleaned, deduped, CDC applied (`customers_scd2`, `products_scd1`, `inventory_scd1`, `orders`, `clicks`), plus joined `click_to_order`
- `ecom_lab.gold`: `revenue_5min`, `funnel_daily`, `customer_ltv`, `low_stock_alerts`
- `ecom_lab.ops`: test results, quarantine tables, expectation metrics

> Diagram (in the source doc): lab pipeline · 5 stages, replay loop

Governance policies sit on silver and gold; the dashed loop is how every rewind and backfill scenario rebuilds downstream tables from bronze.

**Scenario IDs:** `LA` late arrival, `JN` joins, `CDC` change data, `RR` rewind and replay, `SE` schema evolution, `GV` governance, `UT`/`IT` unit and integration tests, `AB` asset bundles. Each scenario lists the trigger (generator knob), what should happen, and how to verify it.

## Data model and generator knobs

Four CDC feeds plus one event stream, each written as small JSON files every few seconds so Auto Loader sees a steady trickle.

| Feed | Key | Main fields | PII | Changes over time |
| --- | --- | --- | --- | --- |
| customers (CDC) | customer_id | name, email, phone, address, province, loyalty_tier, created_at | name, email, phone, address | address moves, tier upgrades, deletes (account closure) |
| products (CDC) | product_id | name, category, price, is_active | none | price changes, discontinued |
| inventory (CDC) | product_id, warehouse_id | qty_on_hand, updated_at | none | qty drops on each order, restocks |
| orders (CDC) | order_id | customer_id, items[], total, status, order_ts, province | none (joins to PII) | status: placed, paid, shipped, delivered, cancelled, refunded |
| clicks (events) | event_id | session_id, customer_id (nullable), event_type, product_id, event_ts, device, ip | ip, customer_id | append only: view, add_to_cart, checkout |

**CDC envelope (Debezium style):**

```json
{"op": "u", "ts_ms": 1764012345678, "source": {"table": "customers", "lsn": 18233},
 "before": {"customer_id": 42, "province": "ON"},
 "after":  {"customer_id": 42, "province": "BC"}}
```

`op` is `c`, `u`, `d` or `r` (snapshot). `ts_ms` is the change time, and the generator guarantees it strictly increases per row, so silver sequences by it: the flatten view exposes `timestamp_millis(ts_ms) AS change_ts` and AUTO CDC uses `SEQUENCE BY change_ts`. That makes `__START_AT`/`__END_AT` timestamps. `source.lsn` is the database log position; it stays in bronze for debugging. (With real Debezium, prefer `source.ts_ms`, the commit time; the top-level `ts_ms` is when the connector processed the change.)

**Generator knobs (one parameter dict per run)**

| Knob | Example | What it produces |
| --- | --- | --- |
| `rate_per_sec` | 20 | Steady event volume |
| `late_pct`, `late_by` | 5%, [2m, 15m, 2h] | Events whose event time is in the past |
| `early_fact_pct` | 3% | Orders or clicks for a customer_id not created yet |
| `late_dim_delay` | 10m | How long until that customer row shows up |
| `orphan_pct` | 1% | Facts whose dimension never arrives |
| `out_of_order_pct` | 5% | An older change written after a newer one (earlier `ts_ms`, lower lsn) |
| `dup_pct` | 2% | Exact replays of earlier events (simulates Debezium failover) |
| `delete_before_insert` | on/off | A `d` arriving before its `c` |
| `bad_record_pct` | 1% | Null keys, negative totals, malformed JSON |
| `schema_version` | v1 or v2 | v2 adds `orders.discount_code` and `customers.marketing_opt_in` |
| `clock_skew` | +/- 90s | Event time ahead of ingest time (future-dated events) |
| `seed` | 42 | Reproducible runs for tests |

The `seed` knob matters: the same seed must produce the same files, so integration tests can assert exact counts.

## Late and early arrival

The gold table `revenue_5min` uses `withWatermark("order_ts", "10 minutes")` and a 5-minute tumbling window. Every scenario below is tested against it.

| ID | Scenario | Trigger | Expected | Verify |
| --- | --- | --- | --- | --- |
| LA-01 | On-time baseline | `late_pct=0` | Every order counted once in its window | Gold sum = bronze sum for closed windows |
| LA-02 | Late within watermark | `late_by=2m` | Event lands in its original window; window updated | Window total changes between two runs |
| LA-03 | Late beyond watermark | `late_by=2h` | Dropped from gold silently; still in bronze and silver | Bronze minus gold diff = exactly the 2h events |
| LA-04 | Append vs update output mode | Run LA-02 twice, once per mode | Append: window emitted only after watermark passes. Update: emitted early and revised | Compare row counts and emit times |
| LA-05 | Future-dated events | `clock_skew=+90s` | Watermark jumps ahead, so normal events now look late | Watch `numRowsDroppedByWatermark` rise |
| LA-06 | Idle stream | Stop the generator for 20 min | Last window never closes in append mode (no new events move the watermark) | Last window missing until data resumes |
| LA-07 | Multiple watermarks | Join clicks (1m) and orders (10m) | Global watermark = slowest stream by default (`min` policy) | Check `eventTime.watermark` in query progress |
| LA-08 | Late fix via batch | Re-run gold from silver as batch for one day | Late events from LA-03 now included | Day total matches bronze |

**Where to look:** `query.lastProgress` (`eventTime.watermark`, `stateOperators[].numRowsDroppedByWatermark`) for plain Structured Streaming, and the pipeline event log for Lakeflow pipelines.

**Exam angle:** know that a watermark bounds state and decides lateness, and that append mode waits for the watermark.

## Joins

Two join families get tested: orders enriched with customers (stream-static, the dimension is a Delta table re-read each micro-batch) and clicks matched to orders (stream-stream, both sides buffered in state).

| ID | Scenario | Trigger | Expected | Verify |
| --- | --- | --- | --- | --- |
| JN-01 | Stream-static inner, dimension present | Baseline | Every order enriched | Null customer fields = 0 |
| JN-02 | Stream-static inner, early fact | `early_fact_pct=3%`, `late_dim_delay=10m` | Those orders dropped for good; the later customer row does not bring them back | Silver orders count < bronze orders count |
| JN-03 | Stream-static left, early fact | Same as JN-02 with a left join | Orders kept with null customer columns, never refilled | Count rows where `customer_name IS NULL` |
| JN-04 | Repair early facts | Run a MERGE (or batch job) after the customer arrives | Nulls from JN-03 filled in | Null count drops to orphans only |
| JN-05 | Placeholder dimension | Insert an `unknown` customer row when an order arrives first, update it later | Gold joins never lose orders; SCD2 shows placeholder then real version | Two versions for that customer_id |
| JN-06 | Stream-stream inner, within bound | Click then order within 30 min | Joined row emitted when the order arrives | Row present in `click_to_order` |
| JN-07 | Stream-stream inner, order late beyond bound | Order 2h after click | No match, click state evicted after watermark | Click absent from joined output |
| JN-08 | Stream-stream left outer | Click with no order | Null-padded row emitted only after watermark passes the time bound | Note the delay between click time and emit time |
| JN-09 | No watermark, no time bound | Remove both | Query fails to start for outer joins; inner join runs but state grows without limit | Watch `numRowsTotal` in state metrics keep climbing |
| JN-10 | Orphans | `orphan_pct=1%` | Left join keeps them forever with nulls; inner drops them | Route nulls to `ops.orphan_orders` with an expectation |
| JN-11 | Dimension changes mid-stream | Customer moves ON to BC between two orders | Stream-static uses the latest version at processing time, not order time | Compare with a point-in-time join to `customers_scd2` (`order_ts BETWEEN __START_AT AND __END_AT`) |

The JN-11 contrast is the one most people miss: stream-static joins give you the dimension as it is now, so historically correct numbers need an SCD2 point-in-time join in batch or gold.

## CDC

Bronze keeps the raw envelope. A flatten view picks `after` (or `before` for deletes) and exposes `op` and `change_ts` (`timestamp_millis(ts_ms)`). `AUTO CDC` (the newer name for `APPLY CHANGES`) then builds silver.

```sql
CREATE OR REFRESH STREAMING TABLE silver.customers_scd2;

CREATE FLOW customers_cdc AS AUTO CDC INTO silver.customers_scd2
FROM STREAM(bronze.customers_flat)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts, _ingest_ts, _source_file)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (loyalty_points);
```

| ID | Scenario | Trigger | Expected | Verify |
| --- | --- | --- | --- | --- |
| CDC-01 | Initial snapshot | First run with `op='r'` rows | One current row per key | Count = distinct keys |
| CDC-02 | Out-of-order update | `out_of_order_pct=5%` | Later `change_ts` wins; older update ignored | Final value = latest-`change_ts` value |
| CDC-03 | Duplicate replay | `dup_pct=2%` | No extra rows or versions | SCD2 version count unchanged after replay |
| CDC-04 | Delete then re-insert | `d` then `c` with a later `ts_ms` | SCD1: row returns. SCD2: old version closed, new one opened | Check `__END_AT` on the closed version |
| CDC-05 | Delete before insert | `delete_before_insert=on` | Tombstone wins; late, older insert does not revive the row | Key absent from SCD1 |
| CDC-06 | SCD1 vs SCD2 side by side | Same feed into `customers_scd1` and `customers_scd2` | SCD1 = current only; SCD2 = full history | Row counts and `__START_AT`/`__END_AT` |
| CDC-07 | History on some columns only | `TRACK HISTORY ON` excluding `loyalty_points` | Points changes update in place, no new version | Version count after a points-only change |
| CDC-08 | Null key or bad op | `bad_record_pct=1%` | Rejected by expectation before AUTO CDC | Rows in quarantine, pipeline still running |
| CDC-09 | Order status lifecycle | placed to refunded | SCD1 orders show latest status; gold revenue nets refunds | Revenue drops after refund event |
| CDC-10 | Point-in-time query | Ask: what was customer 42's province on a past date | One row from SCD2 by date range | Matches the change you generated |

**Without AUTO CDC:** do CDC-02 and CDC-03 once with a hand-written `foreachBatch` MERGE (dedupe by key keeping the latest `change_ts` first). It shows why `SEQUENCE BY` matters: a naive MERGE lets an old update overwrite a new one.

## Rewind and replay

Bronze is the replay source: it is append-only and keeps every file's contents, so silver and gold can always be rebuilt from it. The scenarios below start with a deliberate mistake, then recover it.

| ID | Scenario | How to break it | How to recover | Verify |
| --- | --- | --- | --- | --- |
| RR-01 | Inspect history | Run a few batches | `DESCRIBE HISTORY`; query `VERSION AS OF` and `TIMESTAMP AS OF` | Counts differ by version as expected |
| RR-02 | Bad deploy corrupts gold | Ship a bug (e.g. revenue x 100) | Plain Delta table: `RESTORE TABLE ... TO VERSION AS OF n`, then fix and rerun | Totals match pre-bug version |
| RR-03 | Same bug in a pipeline table | Bug in a pipeline-managed gold table | Fix the code, then full refresh only that table (selective refresh) | Gold rebuilt from silver, history kept in event log |
| RR-04 | Replay a stream from a version | Need to reprocess silver since yesterday | New checkpoint + `startingVersion` (or `startingTimestamp`) on the Delta source | Only rows after that version reprocessed |
| RR-05 | Auto Loader replay | Delete or change the bronze checkpoint | All files re-ingested; expect duplicates in bronze | Dedupe downstream absorbs them (ties to CDC-03) |
| RR-06 | Change Data Feed | Enable `delta.enableChangeDataFeed` on silver orders | Read changes with `table_changes()` or `readChangeFeed`; feed only changed rows to gold | `_change_type` shows insert, update_preimage, update_postimage, delete |
| RR-07 | Restore breaks downstream streams | `RESTORE` silver while gold streams from it | Gold stream errors on non-append commit; restart with `skipChangeCommits` or full refresh gold | Note which option avoids double counting |
| RR-08 | Time travel limit | `VACUUM` with short retention (test table only) | Old versions no longer readable | Error on `VERSION AS OF` older than retention |
| RR-09 | Backfill a late batch | Drop a day of old files into landing | One-time append flow (`ONCE`) or Auto Loader picks them up normally | Day total in gold corrected |
| RR-10 | Audit a change | Customer complains about a price | Diff two versions of `products_scd1`, or read the SCD2 history | You can show who changed what and when |

**Rules of thumb:** restore is for plain Delta tables, full refresh is for pipeline tables, and a new checkpoint is how you replay a stream. Default retention is 7 days for deleted files and 30 days for the log, so time travel is not a long-term backup.

## Schema evolution and new columns with history

The key rule: a **streaming table** processes each record once, so a new column or new logic only applies to data arriving after the change. A **materialized view** is recomputed from its sources, so it picks up history automatically. Backfilling a streaming table means a full refresh or a separate backfill.

| ID | Scenario | Trigger | Expected | How to get history | Verify |
| --- | --- | --- | --- | --- | --- |
| SE-01 | New source column, Auto Loader | `schema_version=v2` (adds `discount_code`) | With `addNewColumns` (default) the stream stops once, then restarts with the new column | Old rows stay null: correct, the field did not exist | `DESCRIBE` bronze; schema location has the new version |
| SE-02 | Rescue mode | `schemaEvolutionMode=rescue` | Schema stays fixed; new field lands in `_rescued_data` | Parse `_rescued_data` later to promote it | Count non-null `_rescued_data` |
| SE-03 | Type mismatch | Generator sends `total` as a string | Value goes to `_rescued_data`, not a failure | Fix with a cast in silver | Rescued count = bad rows |
| SE-04 | Derived column on silver streaming table | Add `order_size = CASE ...` to silver orders | Only new rows get it | Full refresh silver orders (replays all of bronze) | Null count = 0 after refresh |
| SE-05 | Same column on a materialized view | Add it to gold `customer_ltv` | Recomputed for all history on next update | Nothing extra needed | All rows filled |
| SE-06 | New tracked column in SCD2 | Add `marketing_opt_in` to customers | Versions before the change have null | Full refresh the AUTO CDC target to rebuild history from bronze | Version count may change; check old versions |
| SE-07 | Plain Delta, no pipeline | `ALTER TABLE ADD COLUMN`, then backfill | New column null until backfilled | `UPDATE` or `MERGE` from bronze for past rows | Spot-check old orders |
| SE-08 | Write with new columns | Batch write with an extra column | Fails unless `mergeSchema` is set | n/a | Error, then success with option |
| SE-09 | Rename or drop a column | Rename `province` to `region` | Needs column mapping enabled; downstream streams may need a restart | Test downstream reads | Old queries fail, new ones work |
| SE-10 | New gold metric over all time | Add `discount_total` to `revenue_5min` | If gold is a streaming aggregate, old windows lack it | Recompute as MV or full refresh | Past windows show values |

SE-04 vs SE-05 is the comparison to remember for the exam: choose a materialized view when logic will change and history must follow; choose a streaming table when you need low latency on append-only input.

## Governance

PII lives in `customers` (name, email, phone, address) and `clicks` (ip). Policies are Unity Catalog SQL functions attached to silver and gold tables, so every reader sees the same rules.

```sql
-- Who sees which province (works without account groups on Free Edition)
CREATE TABLE ecom_lab.ops.user_region_access (user_email STRING, province STRING);

CREATE FUNCTION ecom_lab.ops.province_filter(p STRING) RETURN
  EXISTS (SELECT 1 FROM ecom_lab.ops.user_region_access
          WHERE user_email = current_user() AND (province = p OR province = 'ALL'));

CREATE FUNCTION ecom_lab.ops.mask_email(e STRING) RETURN
  CASE WHEN is_account_group_member('pii_readers') THEN e
       ELSE concat('***@', split(e, '@')[1]) END;

ALTER TABLE ecom_lab.silver.customers_scd2 SET ROW FILTER ecom_lab.ops.province_filter ON (province);
ALTER TABLE ecom_lab.silver.customers_scd2 ALTER COLUMN email SET MASK ecom_lab.ops.mask_email;
```

| ID | Scenario | Setup | Expected | Verify |
| --- | --- | --- | --- | --- |
| GV-01 | Mask email, phone, address | Column masks on silver customers | Non-PII readers see `***@gmail.com`, last 4 of phone, city only | Query as yourself with and without the mapping row |
| GV-02 | Row filter by province | `province_filter` on customers and orders | Ontario-only user sees ON rows only | Row counts per user |
| GV-03 | Filter on a pipeline table | Declare `WITH ROW FILTER` and `MASK` in the pipeline's `CREATE` statement | Policy survives full refresh | Run RR-03, then re-check GV-02 |
| GV-04 | Hash instead of mask for joins | `sha2(email, 256)` as `email_hash` in silver | Analysts can join and count distinct without seeing email | Same hash across tables |
| GV-05 | Tag PII columns | `ALTER TABLE ... ALTER COLUMN email SET TAGS ('pii' = 'email')` | Tags searchable in Catalog Explorer | `information_schema.column_tags` |
| GV-06 | Least-privilege grants | `USE CATALOG`, `USE SCHEMA`, `SELECT` on gold only for an analyst role | Analyst cannot read bronze | Permission error on bronze |
| GV-07 | Gold without PII | Gold aggregates exclude PII columns entirely | No mask needed at gold | Column list review |
| GV-08 | Right to be forgotten | CDC delete for customer 42 | Gone from SCD1; SCD2 history and bronze still hold PII | Delete from bronze and history too, then `VACUUM` to remove files |
| GV-09 | Lineage | Open the gold table in Catalog Explorer | Lineage graph back to bronze and the volume | Screenshot for notes |
| GV-10 | Mask applied to time travel | Query `VERSION AS OF` an old version | Current mask still applies | Masked output on old version |

GV-08 is a good real-world test: deleting a person means bronze, every SCD2 version, and old Delta files, not just the current row.

## Unit and integration testing

Pipeline decorators are hard to unit test directly, so keep them thin. Put the logic in plain PySpark functions that take and return DataFrames, then unit test those functions and integration test the whole pipeline.

```python
# src/ecommerce/transforms/cdc.py  (pure functions, no pipeline imports)
def flatten_cdc(df):
    return df.selectExpr("op", "timestamp_millis(ts_ms) AS change_ts",
                         "CASE WHEN op = 'd' THEN before ELSE after END AS r").select("op", "change_ts", "r.*")

# src/ecommerce/pipelines/silver/customers.py  (thin wrapper)
from pyspark import pipelines as dp
from ecommerce.transforms.cdc import flatten_cdc

@dp.view
@dp.expect_or_drop("valid_key", "customer_id IS NOT NULL")
def customers_flat():
    return flatten_cdc(spark.readStream.table("bronze.customers_cdc"))
```

**Unit tests (pytest, small DataFrames, run locally or with Databricks Connect)**

| ID | Function | Case | Assert |
| --- | --- | --- | --- |
| UT-01 | `flatten_cdc` | Insert, update, delete envelopes | Delete row takes values from `before` |
| UT-02 | `dedupe_latest` | Two events for one key, `change_ts` 10:05 and 10:03 | Only 10:05 kept |
| UT-03 | `mask_phone` (Python twin of the SQL mask) | `519-555-1234` | `***-***-1234` |
| UT-04 | `order_size_bucket` | Totals 0, 49.99, 50, 500 | Bucket edges correct |
| UT-05 | `revenue_window` | Static DataFrame with late rows | Window sums match hand calc |
| UT-06 | Schema contract | v1 and v2 samples | Required columns present, types match |
| UT-07 | Generator | Same seed twice | Identical output |

**Integration tests (real pipeline, test schema, fixed seed)**

| ID | Scenario | Steps | Assert |
| --- | --- | --- | --- |
| IT-01 | End to end happy path | Seed 42, 1,000 events, triggered run | Bronze, silver, gold counts equal the known numbers for seed 42 |
| IT-02 | Expectations | `bad_record_pct=5%` | Dropped count in the event log = bad rows generated; pipeline still succeeds |
| IT-03 | Fail fast | `expect_or_fail` on order total >= 0 | Update fails on first negative total |
| IT-04 | Idempotent rerun | Run twice with no new files | No row changes on second run |
| IT-05 | Late and duplicate mix | `late_pct`, `dup_pct`, `out_of_order_pct` all on | Silver matches an expected snapshot table |
| IT-06 | Full refresh equals incremental | Run in 5 batches, then full refresh a copy | Both gold tables identical (`EXCEPT ALL` empty both ways) |
| IT-07 | Governance | Query as test identity | Masked columns and filtered rows as designed |
| IT-08 | Schema v2 rollout | Switch generator to v2 mid-test | Pipeline recovers; new columns present |

Read expectation results from the pipeline event log (`event_type = 'flow_progress'`, `details:flow_progress.data_quality`) rather than counting by hand. IT-06 is the strongest single test you can write: it proves your incremental logic and a rebuild agree.

## Asset bundles (DABs)

One bundle holds the generator job, the pipeline, and the test job, with `dev` and `test` targets pointing at different schemas. Everything is deployed with the Databricks CLI.

```
ecommerce-cdc-databricks/
  databricks.yml
  resources/
    ecom_pipeline.yml          # Lakeflow pipeline (serverless)
    generator_job.yml          # writes JSON to the landing volume
    integration_test_job.yml
  src/ecommerce/
    generator/generate_events.py
    pipelines/{bronze,silver,gold}/
    governance/policies.sql
    transforms/                # pure PySpark functions
  tests/unit/                  # pytest
  tests/integration/           # assertion notebooks
```

```yaml
# databricks.yml (trimmed)
bundle:
  name: ecommerce-cdc-databricks
variables:
  catalog: {default: ecom_lab}
  schema_suffix: {default: dev}
include:
  - resources/*.yml
targets:
  dev:
    mode: development
    default: true
  test:
    variables:
      schema_suffix: test
```

| ID | Scenario | Command or change | Expected |
| --- | --- | --- | --- |
| AB-01 | Validate | `databricks bundle validate -t dev` | Config errors caught before deploy |
| AB-02 | Deploy to dev | `databricks bundle deploy -t dev` | Resources prefixed `[dev <you>]`, schedules paused (development mode) |
| AB-03 | Run the pipeline | `databricks bundle run ecom_pipeline -t dev` | Triggered update; output in dev schemas |
| AB-04 | Full refresh from CLI | `databricks bundle run ecom_pipeline --full-refresh-all` | Rebuild used in RR-03 and SE-04 |
| AB-05 | Variables per target | Deploy `-t test` | Same code writes to `*_test` schemas |
| AB-06 | Integration test job | Job tasks: generator (seed 42), pipeline, assertions | Job fails if any assertion fails |
| AB-07 | Tear down | `databricks bundle destroy -t test` | Test resources removed |
| AB-08 | CI | GitHub Actions: pytest, then validate, deploy `-t test`, run the test job | Pull request blocked on a failing test |

On Free Edition, stick to serverless compute in the job and pipeline definitions, and authenticate the CLI with `databricks auth login`.

## Suggested build order

Each step reuses the previous one, so you always have a working pipeline to break.

1. **Generator + landing volume.** Seeded, knobs off. Check files land (UT-07).
2. **Bronze with Auto Loader.** All five feeds, schema location set (SE-01 later).
3. **Silver CDC.** Flatten, expectations, AUTO CDC SCD1 and SCD2 (CDC-01 to CDC-06).
4. **Turn on chaos knobs.** Out of order, duplicates, deletes (CDC-02, 03, 05, 08).
5. **Joins.** Stream-static first, then stream-stream with watermarks (JN-01 to JN-09).
6. **Gold + late data.** Windows and watermarks (LA-01 to LA-08).
7. **Governance.** Masks, filters, tags, grants (GV-01 to GV-07).
8. **Break and recover.** Rewind and replay (RR-01 to RR-10).
9. **Evolve.** Schema v2 and new columns with history (SE-01 to SE-10).
10. **Package and test.** Move to a bundle, add unit and integration tests, then CI (UT, IT, AB).

Steps 2, 3, 5 and 6 cover the ingestion and pipeline topics, steps 7 and 8 governance and Delta features, and step 10 deployment. Check each against the current exam guide before your exam date.
