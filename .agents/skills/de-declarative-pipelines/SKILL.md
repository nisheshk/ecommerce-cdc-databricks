---
name: de-declarative-pipelines
description: Apply Spark Declarative Pipeline (SDP) best practices for bronze, silver and gold tables, including naming, comments, table properties, data quality constraints, Auto Loader ingestion and AUTO CDC. Use when creating or modifying tables in Lakeflow Spark Declarative Pipelines. Define tables in SQL; Python only as a thin wrapper around tested transforms.
---

<!-- Adapted from databricks-solutions/genie-code-skills-demo skills/sdp-basics (DB license, see
     THIRD_PARTY_NOTICES.md). Changed: layer-as-schema naming, Python-wrapper rule, CDC section. -->

# SDP Pipeline Basics

Define pipeline tables in SQL. When creating or modifying tables in SDP pipelines, follow these rules.

For copy-paste SQL patterns (bronze Auto Loader, flatten view, AUTO CDC, audit columns, data-quality flag), read [template.md](template.md).

## Pipeline Language

- Tables are SQL: `CREATE OR REFRESH STREAMING TABLE`, `CREATE OR REFRESH MATERIALIZED VIEW`, `CREATE FLOW ... AS AUTO CDC`.
- Python is allowed only as a thin wrapper that calls a pure function from `src/ecommerce/transforms/` (DataFrame in, DataFrame out), so the logic can be unit tested. No business logic inside a `@dp` decorator. See the Python wrapper in [template.md](template.md).

## Table Naming

The medallion layer is the **schema**, not a name prefix. Full rules in [references/naming.md](references/naming.md).

| Layer | Schema | Example |
|-------|--------|---------|
| Bronze | `<catalog>.bronze` | `ecom_lab.bronze.customers` |
| Silver | `<catalog>.silver` | `ecom_lab.silver.customers_scd2` |
| Gold | `<catalog>.gold` | `ecom_lab.gold.revenue_5min` |
| Ops | `<catalog>.ops` | `ecom_lab.ops.orphan_orders` |

Use `lowercase_snake_case`. Never use PascalCase, UPPERCASE, kebab-case, or camelCase.

## Table Types

| Type | When to Use | Syntax |
|------|-------------|--------|
| `STREAMING TABLE` | File ingestion (Auto Loader), CDC, real-time data | `CREATE OR REFRESH STREAMING TABLE` |
| `MATERIALIZED VIEW` | Batch data from existing Delta tables, aggregations, logic that must recompute history | `CREATE OR REFRESH MATERIALIZED VIEW` |

A streaming table processes each record once, so new columns or logic apply only to new data. A materialized view recomputes, so history follows. Choose an MV when logic will change and history must follow.

Do **not** use `CREATE OR REFRESH LIVE TABLE` (deprecated) or the `LIVE.` virtual schema. Reference other datasets in the same pipeline by name or a fully qualified `catalog.schema.table`. See [LIVE schema (legacy)](https://docs.databricks.com/aws/en/ldp/live-schema).

## Comments

Every table MUST have a `COMMENT` clause describing its purpose (patterns in `@de-table-governance`).

## Table Properties

Every table MUST have `TBLPROPERTIES` with at least `quality` and `domain`. Do **not** set `"owner"` -- Databricks reserves that key and raises an error. Table ownership is the pipeline run-as identity ([reserved table property keys](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-tblproperties)).

```sql
TBLPROPERTIES ("quality" = "bronze", "domain" = "ecommerce")
TBLPROPERTIES ("quality" = "silver", "domain" = "ecommerce", "delta.enableChangeDataFeed" = "true", "delta.enableRowTracking" = "true")
TBLPROPERTIES ("quality" = "gold", "domain" = "ecommerce", "delta.enableChangeDataFeed" = "true")
```

## Audit Columns

Every table MUST include `audit_timestamp` and `source_system` as the LAST columns. Bronze also keeps the file metadata `_ingest_ts` and `_source_file` before them. See [template.md](template.md) for the exact expressions.

## Bronze Ingestion

- Ingest landing files with Auto Loader: `FROM STREAM read_files('/Volumes/ecom_lab/landing/raw/cdc/<feed>/', format => 'json')`.
- Keep the raw CDC envelope (`op`, `ts_ms`, `source`, `before`, `after`); flatten in a view, not in bronze.
- Set a schema location and keep `_rescued_data` so new or mistyped fields are not lost.

## CDC

- Flatten with a view: take `after` (or `before` when `op = 'd'`), expose `op` and `timestamp_millis(ts_ms) AS change_ts`.
- Build silver with `AUTO CDC`: `KEYS (<pk>)`, `APPLY AS DELETE WHEN op = 'd'`, `SEQUENCE BY change_ts`, `COLUMNS * EXCEPT (op, change_ts, _ingest_ts, _source_file)`. SCD2 `__START_AT`/`__END_AT` are then timestamps.
- `STORED AS SCD TYPE 1` for current state, `SCD TYPE 2` for history. Exclude fast-changing counters from history with `TRACK HISTORY ON * EXCEPT (...)`.
- Reject null keys and unknown `op` values with constraints on the flatten view, before AUTO CDC.

## Data Quality Constraints

- **Bronze**: Use `WHERE` clause filtering only (preserve raw data)
- **Silver**: Use `CONSTRAINT` clauses for validation
  - `ON VIOLATION FAIL UPDATE` for critical fields
  - `ON VIOLATION DROP ROW` for non-critical fields
  - Route dropped rows you need to inspect to an `ops` quarantine table
- **Gold**: Generally no constraints (data validated in Silver)

## Data Quality Flag

Silver tables MUST include a `data_quality_flag` column. Use the CASE pattern in [template.md](template.md).

## SQL Formatting

| Element | Case |
|---------|------|
| SQL keywords | UPPERCASE (`SELECT`, `FROM`, `WHERE`, `AS`) |
| Table/column names | lowercase_snake_case |
| Aliases | short lowercase (`t`, `a`, `d`) |
| Functions | UPPERCASE (`ROUND()`, `CAST()`, `COALESCE()`) |

## Column Organization Order

1. Identifiers (primary keys, foreign keys)
2. Dimensions (categories, hierarchies)
3. Measures (quantities, amounts)
4. Derived/calculated fields
5. Data quality flags
6. **Audit columns LAST** (`audit_timestamp`, `source_system`)

## Clustering

Add `CLUSTER BY AUTO` for `STREAMING TABLE` definitions.

## Joins and Watermarks

- Reference other datasets in the same pipeline by table name, not `LIVE.<table>`
- Use fully qualified names for tables outside the pipeline
- Read streaming sources with the `STREAM` keyword (`FROM STREAM read_files(...)` or `FROM STREAM source_table`). Do not use `STREAM` when creating a materialized view.
- Always use explicit `JOIN` syntax with table aliases
- Stream-stream joins need a watermark on both sides and a time bound in the join condition
- Stream-static joins see the dimension as it is at processing time; use an SCD2 point-in-time join (`BETWEEN __START_AT AND __END_AT`) when history must be correct
- Gold: `ROUND()` money columns to 2 decimals
