# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce CDC event generator
# MAGIC
# MAGIC Writes seeded CDC and click files into the landing volume (see `src/ecommerce/SPEC.md`).
# MAGIC The logic lives in `generate_events.py` next to this notebook, which the unit tests cover.
# MAGIC
# MAGIC **Two ways to use it**
# MAGIC - **As a job task, or Run all:** set the widgets and the run stops after **Check the output**.
# MAGIC   Job parameters use the widget names (`scenario`, `output_root`, `duration`, ...).
# MAGIC - **Interactively:** run the cells under **Scenario configs** one at a time. Each one is a single knob set
# MAGIC   from the spec.
# MAGIC
# MAGIC Every run restarts IDs from 1. Reset the landing folder (last cell) before switching scenarios, or point
# MAGIC `output_root` at a new folder.

# COMMAND ----------

import json

from generate_events import SCENARIOS, generate, scenario_knobs

# COMMAND ----------

dbutils.widgets.text("output_root", "/Volumes/ecom_lab/landing/raw", "Output root")
dbutils.widgets.dropdown("scenario", "baseline", sorted(SCENARIOS), "Scenario")
dbutils.widgets.text("duration", "", "Duration (blank = scenario default)")
dbutils.widgets.dropdown("realtime", "false", ["false", "true"], "Realtime trickle")
dbutils.widgets.text("seed", "42", "Seed")
dbutils.widgets.text("params", "{}", "Knob overrides (JSON)")

OUTPUT_ROOT = dbutils.widgets.get("output_root").rstrip("/")
REALTIME = dbutils.widgets.get("realtime") == "true"
SEED = int(dbutils.widgets.get("seed"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Creates the catalog, schema and volume in `output_root` if they are missing. Creating the catalog needs
# MAGIC the `CREATE CATALOG` privilege on the metastore.

# COMMAND ----------

# DBTITLE 1,Setup
parts = OUTPUT_ROOT.split("/")
if OUTPUT_ROOT.startswith("/Volumes/") and len(parts) >= 5:
    catalog, schema, volume = parts[2:5]
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")
    print(f"Volume ready: {catalog}.{schema}.{volume}")
    # Medallion schemas the lab pipeline writes to (bronze/silver/gold/ops).
    for layer in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{layer}`")
    print(f"Lab schemas ready in {catalog}: bronze, silver, gold, ops")


def run_scenario(name, overrides=None, duration=None, realtime=REALTIME):
    """Generate one preset into OUTPUT_ROOT. The seed widget applies unless overrides set one."""
    knobs = scenario_knobs(name, {"seed": SEED, **(overrides or {})})
    duration = duration or SCENARIOS[name]["duration"]
    print(f"{name} ({SCENARIOS[name]['covers']}) for {duration}: {knobs}")
    return generate(OUTPUT_ROOT, knobs, duration, realtime=realtime)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the selected scenario

# COMMAND ----------

totals = run_scenario(
    dbutils.widgets.get("scenario"),
    overrides=json.loads(dbutils.widgets.get("params")),
    duration=dbutils.widgets.get("duration") or None,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the output

# COMMAND ----------

from generate_events import FEED_DIRS

display(spark.createDataFrame(
    [(feed, f"{OUTPUT_ROOT}/{folder}", totals[feed]) for feed, folder in FEED_DIRS.items()],
    "feed string, path string, lines_written long",
))

# COMMAND ----------

# Stop here on Run all and in jobs; the cells below are run one at a time.
dbutils.notebook.exit(json.dumps({"scenario": dbutils.widgets.get("scenario"), "totals": totals}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario configs
# MAGIC Run one cell at a time. Each writes into `output_root`, so reset it between scenarios unless you mean to
# MAGIC combine them. Pass `overrides={...}` or `duration="..."` to tweak a preset.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline: all knobs off
# MAGIC LA-01, JN-01, CDC-01, CDC-06, UT-07. Build order steps 1-3.

# COMMAND ----------

run_scenario("baseline")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Late arrival: `late_pct=5%`, `late_by=[2m, 15m, 2h]`
# MAGIC LA-02, LA-03, LA-04, LA-08. Events inside the 10-minute watermark update their window; 2h events are
# MAGIC dropped from gold.

# COMMAND ----------

run_scenario("late_arrival")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Future-dated events: `clock_skew=+90s`
# MAGIC LA-05. The watermark jumps ahead, so normal events start to look late.

# COMMAND ----------

run_scenario("future_dated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Early and orphan facts: `early_fact_pct=3%`, `late_dim_delay=10m`, `orphan_pct=1%`
# MAGIC JN-02 to JN-05, JN-10. Runs 15 minutes so the late customer rows show up.

# COMMAND ----------

run_scenario("early_and_orphan_facts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDC chaos: `out_of_order_pct=5%`, `dup_pct=2%`, `delete_before_insert=on`
# MAGIC CDC-02 to CDC-05. Build order step 4.

# COMMAND ----------

run_scenario("cdc_chaos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bad records: `bad_record_pct=5%`
# MAGIC CDC-08, IT-02, IT-03. Null keys, a bad `op`, negative totals and malformed JSON.

# COMMAND ----------

run_scenario("bad_records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema v2: `schema_version=v2`
# MAGIC SE-01, SE-02, SE-06, IT-08. Adds `orders.discount_code` and `customers.marketing_opt_in`. Run after a v1
# MAGIC scenario, without resetting, to see Auto Loader pick up the new columns.

# COMMAND ----------

run_scenario("schema_v2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Integration happy path: seed 42, 1,000 events
# MAGIC IT-01, IT-04, IT-06. Fixed seed and start time, so the counts are the same every run.

# COMMAND ----------

run_scenario("integration_happy_path", overrides={"seed": 42}, realtime=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Integration chaos mix: late, duplicate and out-of-order all on
# MAGIC IT-05.

# COMMAND ----------

run_scenario("integration_chaos_mix", overrides={"seed": 42}, realtime=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Custom
# MAGIC Start from any preset and set your own knobs.

# COMMAND ----------

run_scenario("baseline", overrides={"rate_per_sec": 50, "late_pct": "10%", "late_by": ["5m"]}, duration="2m")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reset the landing folder
# MAGIC Deletes everything under `output_root`. Bronze tables and checkpoints are not touched: full refresh the
# MAGIC pipeline after a reset.

# COMMAND ----------

dbutils.fs.rm(OUTPUT_ROOT, recurse=True)
print(f"Removed {OUTPUT_ROOT}")