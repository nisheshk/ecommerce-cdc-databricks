<!-- Adapted from databricks-solutions/genie-code-skills-demo skills/sdp-basics/sql-templates.md (DB license,
     see THIRD_PARTY_NOTICES.md). Changed: e-commerce CDC templates, flatten view, AUTO CDC, Python wrapper. -->

# SDP SQL templates

Reusable patterns for `@de-declarative-pipelines`. Substitute entity names, source and quality tags.

## Audit columns (always last)

```sql
current_timestamp() AS audit_timestamp,
'<source_description>' AS source_system
```

## Data quality flag (silver)

```sql
CASE
  WHEN <field> IS NULL THEN 'MISSING_<FIELD>'
  WHEN <field> < 0 THEN 'NEGATIVE_<FIELD>'
  ELSE 'CLEAN'
END AS data_quality_flag
```

## Bronze streaming table (Auto Loader, raw CDC envelope)

```sql
CREATE OR REFRESH STREAMING TABLE ecom_lab.bronze.orders
COMMENT "Raw order CDC events ingested from /Volumes/ecom_lab/landing/raw/cdc/orders"
TBLPROPERTIES ("quality" = "bronze", "domain" = "ecommerce", "delta.enableRowTracking" = "true")
CLUSTER BY AUTO
AS SELECT
  *,
  _metadata.file_modification_time AS _ingest_ts,
  _metadata.file_path AS _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM read_files(
  "/Volumes/ecom_lab/landing/raw/cdc/orders/",
  format => "json"
);
```

## Flatten view with constraints (before AUTO CDC)

```sql
CREATE TEMPORARY VIEW orders_flat(
  CONSTRAINT valid_op EXPECT (op IN ('c', 'u', 'd', 'r')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_key EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  op,
  source.lsn AS lsn,
  CASE WHEN op = 'd' THEN before.order_id ELSE after.order_id END AS order_id,
  CASE WHEN op = 'd' THEN before.customer_id ELSE after.customer_id END AS customer_id,
  CASE WHEN op = 'd' THEN before.total ELSE after.total END AS total,
  CASE WHEN op = 'd' THEN before.status ELSE after.status END AS status,
  CAST(CASE WHEN op = 'd' THEN before.order_ts ELSE after.order_ts END AS TIMESTAMP) AS order_ts,
  _ingest_ts,
  _source_file,
  audit_timestamp,
  source_system
FROM STREAM(ecom_lab.bronze.orders);
```

## AUTO CDC into silver (SCD1)

```sql
CREATE OR REFRESH STREAMING TABLE ecom_lab.silver.orders
COMMENT "Current state of orders (SCD1) from bronze.orders via AUTO CDC"
TBLPROPERTIES ("quality" = "silver", "domain" = "ecommerce", "delta.enableChangeDataFeed" = "true");

CREATE FLOW orders_cdc AS AUTO CDC INTO ecom_lab.silver.orders
FROM STREAM(orders_flat)
KEYS (order_id)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY lsn
COLUMNS * EXCEPT (op, lsn, _ingest_ts, _source_file)
STORED AS SCD TYPE 1;
```

## Gold materialized view

```sql
CREATE OR REFRESH MATERIALIZED VIEW ecom_lab.gold.revenue_daily
COMMENT "Business aggregation: net revenue by day and province from silver.orders"
TBLPROPERTIES ("quality" = "gold", "domain" = "ecommerce", "delta.enableChangeDataFeed" = "true")
AS SELECT
  DATE(o.order_ts) AS order_date,
  o.province,
  COUNT(*) AS order_count,
  ROUND(SUM(CASE WHEN o.status = 'refunded' THEN 0 ELSE o.total END), 2) AS net_revenue,
  current_timestamp() AS audit_timestamp,
  'gold_aggregation' AS source_system
FROM ecom_lab.silver.orders o
WHERE o.status <> 'cancelled'
GROUP BY DATE(o.order_ts), o.province;
```

## Python thin wrapper (only when the logic needs unit tests)

```python
from pyspark import pipelines as dp
from ecommerce.transforms.cdc import flatten_cdc  # pure function, unit tested


@dp.temporary_view(comment="Flattened customer CDC events from bronze.customers")
@dp.expect_or_drop("valid_key", "customer_id IS NOT NULL")
def customers_flat():
    return flatten_cdc(spark.readStream.table("ecom_lab.bronze.customers"))
```
