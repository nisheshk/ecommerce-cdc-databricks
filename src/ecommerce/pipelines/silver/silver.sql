-- ============================================================================
-- Silver layer (SPEC.md build order step 3): cleaned, validated, CDC applied.
-- SCD types per SPEC:
--   customers -> SCD TYPE 2  (history: address moves, tier upgrades, account
--                closures; point-in-time joins for gold, CDC-10 / JN-11).
--                loyalty_points is tracked in place (CDC-07).
--   products  -> SCD TYPE 1  (price changes are corrections; latest state only)
--   inventory  -> SCD TYPE 1  (current stock per product_id + warehouse_id)
--   orders     -> SCD TYPE 1  (order_id key; latest status, CDC-09)
--   clicks     -> append-only events, no CDC
--
-- Pattern per CDC feed: <feed>_raw (unvalidated flatten view) feeds BOTH
-- ops.cdc_quarantine (invalid rows are kept) and <feed>_flat (CONSTRAINT
-- expectations drop invalid rows) which feeds AUTO CDC. CONSTRAINT clauses on
-- views use the current CREATE TEMPORARY VIEW syntax (CDC-08).
--
-- PII (GV-01 to GV-04): raw values stay in silver behind UC column masks and
-- the province row filter, both DECLARED IN THE CREATE so they survive a full
-- refresh (GV-03). email_hash (sha2 256) enables PII-free joins (GV-04).
-- The mask/row-filter functions live in ecom_lab.ops (governance/policies.sql).
--
-- Column comments + UC tags: pipeline source may only contain dataset
-- definitions, so ALTER TABLE ... COMMENT / SET TAGS statements live in
-- silver_tags.sql and are applied after the tables exist (re-run after any
-- full refresh).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Flatten views (raw, unvalidated) - keep the Debezium envelope work shared by
-- validation and quarantine. r = the row image (after, or before when op = 'd').
-- ----------------------------------------------------------------------------
CREATE TEMPORARY VIEW customers_raw AS
SELECT
  op,
  source.lsn AS lsn,
  timestamp_millis(ts_ms) AS change_ts,
  CASE WHEN op = 'd' THEN before ELSE after END AS r,
  _ingest_ts,
  _source_file
FROM STREAM(ecom_lab.bronze.customers);

CREATE TEMPORARY VIEW products_raw AS
SELECT
  op,
  source.lsn AS lsn,
  timestamp_millis(ts_ms) AS change_ts,
  CASE WHEN op = 'd' THEN before ELSE after END AS r,
  _ingest_ts,
  _source_file
FROM STREAM(ecom_lab.bronze.products);

CREATE TEMPORARY VIEW inventory_raw AS
SELECT
  op,
  source.lsn AS lsn,
  timestamp_millis(ts_ms) AS change_ts,
  CASE WHEN op = 'd' THEN before ELSE after END AS r,
  _ingest_ts,
  _source_file
FROM STREAM(ecom_lab.bronze.inventory);

CREATE TEMPORARY VIEW orders_raw AS
SELECT
  op,
  source.lsn AS lsn,
  timestamp_millis(ts_ms) AS change_ts,
  CASE WHEN op = 'd' THEN before ELSE after END AS r,
  _ingest_ts,
  _source_file
FROM STREAM(ecom_lab.bronze.orders);

-- ----------------------------------------------------------------------------
-- Validated flatten views - null keys and unknown ops are dropped before
-- AUTO CDC (CDC-08); data_quality_flag records per-row cleaning status.
-- ----------------------------------------------------------------------------
CREATE TEMPORARY VIEW customers_flat (
  CONSTRAINT valid_customer_key EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_op EXPECT (op IN ('c', 'u', 'd', 'r')) ON VIOLATION DROP ROW
) AS
SELECT
  op,
  change_ts,
  r.customer_id AS customer_id,
  r.name AS name,                       -- [PII: NAME - MEDIUM]
  r.email AS email,                     -- [PII: EMAIL - HIGH]
  r.phone AS phone,                     -- [PII: PHONE - HIGH]
  r.address AS address,                 -- [PII: ADDRESS - MEDIUM]
  r.province AS province,
  r.loyalty_tier AS loyalty_tier,
  r.loyalty_points AS loyalty_points,
  CAST(r.created_at AS TIMESTAMP) AS created_at,
  sha2(r.email, 256) AS email_hash,     -- join key without PII exposure (GV-04)
  CASE
    WHEN r.email IS NULL THEN 'MISSING_EMAIL'
    WHEN r.phone IS NULL THEN 'MISSING_PHONE'
    WHEN r.name IS NULL THEN 'MISSING_NAME'
    WHEN r.address IS NULL THEN 'MISSING_ADDRESS'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  _ingest_ts,
  _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM(customers_raw);

CREATE TEMPORARY VIEW products_flat (
  CONSTRAINT valid_product_key EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_op EXPECT (op IN ('c', 'u', 'd', 'r')) ON VIOLATION DROP ROW
) AS
SELECT
  op,
  change_ts,
  r.product_id AS product_id,
  r.name AS name,
  r.category AS category,
  r.price AS price,
  r.is_active AS is_active,
  CASE
    WHEN r.name IS NULL THEN 'MISSING_NAME'
    WHEN r.price IS NULL THEN 'MISSING_PRICE'
    WHEN r.price < 0 THEN 'NEGATIVE_PRICE'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  _ingest_ts,
  _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM(products_raw);

CREATE TEMPORARY VIEW inventory_flat (
  CONSTRAINT valid_inventory_key EXPECT (product_id IS NOT NULL AND warehouse_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_op EXPECT (op IN ('c', 'u', 'd', 'r')) ON VIOLATION DROP ROW
) AS
SELECT
  op,
  change_ts,
  r.product_id AS product_id,
  r.warehouse_id AS warehouse_id,
  r.qty_on_hand AS qty_on_hand,
  CAST(r.updated_at AS TIMESTAMP) AS updated_at,
  CASE
    WHEN r.qty_on_hand IS NULL THEN 'MISSING_QTY'
    WHEN r.qty_on_hand < 0 THEN 'NEGATIVE_QTY'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  _ingest_ts,
  _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM(inventory_raw);

CREATE TEMPORARY VIEW orders_flat (
  CONSTRAINT valid_order_key EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_op EXPECT (op IN ('c', 'u', 'd', 'r')) ON VIOLATION DROP ROW
) AS
SELECT
  op,
  change_ts,
  r.order_id AS order_id,
  r.customer_id AS customer_id,
  r.items AS items,
  r.total AS total,
  r.status AS status,
  CAST(r.order_ts AS TIMESTAMP) AS order_ts,
  r.province AS province,
  CASE
    WHEN r.total IS NULL THEN 'MISSING_TOTAL'
    WHEN r.total < 0 THEN 'NEGATIVE_TOTAL'
    WHEN r.customer_id IS NULL THEN 'MISSING_CUSTOMER'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  _ingest_ts,
  _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM(orders_raw);

-- ----------------------------------------------------------------------------
-- Quarantine: every row rejected by the expectations above, from all five
-- feeds, kept for inspection and replay (CDC-08). The rejected customer rows
-- carry PII inside the JSON record, hence the confidential classification.
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ecom_lab.ops.cdc_quarantine (
  feed STRING COMMENT 'Feed the rejected row came from: customers, products, inventory, orders or clicks',
  reject_reason STRING COMMENT 'Why the row was rejected: null_key, unknown_op, or a combination',
  op STRING COMMENT 'Debezium operation of the rejected envelope; NULL for clicks',
  lsn BIGINT COMMENT 'Log sequence number of the rejected event; NULL for clicks',
  record STRING COMMENT 'Rejected record as JSON - CONTAINS PII when the source feed does',
  _ingest_ts TIMESTAMP COMMENT 'Landing file modification time captured by Auto Loader',
  _source_file STRING COMMENT 'Landing file path captured by Auto Loader',
  audit_timestamp TIMESTAMP COMMENT 'Pipeline execution timestamp',
  source_system STRING COMMENT 'Upstream source system identifier'
)
COMMENT 'Rejected rows (null keys, unknown ops, invalid click events) from all feeds, kept for inspection and replay - CONTAINS PII: record (JSON of source row)'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain' = 'ecommerce',
  'contains_pii' = 'true',
  'pii_columns' = 'record',
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY AUTO;

CREATE FLOW quarantine_customers AS
INSERT INTO ecom_lab.ops.cdc_quarantine BY NAME
SELECT
  'customers' AS feed,
  CASE
    WHEN r.customer_id IS NULL AND (op IS NULL OR op NOT IN ('c', 'u', 'd', 'r')) THEN 'null_key_and_unknown_op'
    WHEN r.customer_id IS NULL THEN 'null_key'
    ELSE 'unknown_op'
  END AS reject_reason,
  op,
  lsn,
  to_json(r) AS record,
  _ingest_ts,
  _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM(customers_raw)
WHERE r.customer_id IS NULL OR op IS NULL OR op NOT IN ('c', 'u', 'd', 'r');

CREATE FLOW quarantine_products AS
INSERT INTO ecom_lab.ops.cdc_quarantine BY NAME
SELECT
  'products' AS feed,
  CASE
    WHEN r.product_id IS NULL AND (op IS NULL OR op NOT IN ('c', 'u', 'd', 'r')) THEN 'null_key_and_unknown_op'
    WHEN r.product_id IS NULL THEN 'null_key'
    ELSE 'unknown_op'
  END AS reject_reason,
  op,
  lsn,
  to_json(r) AS record,
  _ingest_ts,
  _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM(products_raw)
WHERE r.product_id IS NULL OR op IS NULL OR op NOT IN ('c', 'u', 'd', 'r');

CREATE FLOW quarantine_inventory AS
INSERT INTO ecom_lab.ops.cdc_quarantine BY NAME
SELECT
  'inventory' AS feed,
  CASE
    WHEN r.product_id IS NULL AND (op IS NULL OR op NOT IN ('c', 'u', 'd', 'r')) THEN 'null_key_and_unknown_op'
    WHEN r.product_id IS NULL THEN 'null_key'
    ELSE 'unknown_op'
  END AS reject_reason,
  op,
  lsn,
  to_json(r) AS record,
  _ingest_ts,
  _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM(inventory_raw)
WHERE r.product_id IS NULL OR r.warehouse_id IS NULL OR op IS NULL OR op NOT IN ('c', 'u', 'd', 'r');

CREATE FLOW quarantine_orders AS
INSERT INTO ecom_lab.ops.cdc_quarantine BY NAME
SELECT
  'orders' AS feed,
  CASE
    WHEN r.order_id IS NULL AND (op IS NULL OR op NOT IN ('c', 'u', 'd', 'r')) THEN 'null_key_and_unknown_op'
    WHEN r.order_id IS NULL THEN 'null_key'
    ELSE 'unknown_op'
  END AS reject_reason,
  op,
  lsn,
  to_json(r) AS record,
  _ingest_ts,
  _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM(orders_raw)
WHERE r.order_id IS NULL OR op IS NULL OR op NOT IN ('c', 'u', 'd', 'r');

CREATE FLOW quarantine_clicks AS
INSERT INTO ecom_lab.ops.cdc_quarantine BY NAME
SELECT
  'clicks' AS feed,
  CASE
    WHEN event_id IS NULL AND (event_type IS NULL OR event_type NOT IN ('view', 'add_to_cart', 'checkout')) THEN 'null_key_and_unknown_event_type'
    WHEN event_id IS NULL THEN 'null_key'
    ELSE 'unknown_event_type'
  END AS reject_reason,
  CAST(NULL AS STRING) AS op,
  CAST(NULL AS BIGINT) AS lsn,
  to_json(struct(event_id, session_id, customer_id, event_type, product_id, event_ts, device, ip)) AS record,
  _ingest_ts,
  _source_file,
  current_timestamp() AS audit_timestamp,
  'web_clickstream' AS source_system
FROM STREAM(ecom_lab.bronze.clicks)
WHERE event_id IS NULL OR event_type IS NULL OR event_type NOT IN ('view', 'add_to_cart', 'checkout');

-- ----------------------------------------------------------------------------
-- customers_scd2 (CDC) - SCD TYPE 2 - CONTAINS PII: name, email, phone, address
-- Masks and the province row filter are declared here so they survive a full
-- refresh (GV-02, GV-03). __START_AT/__END_AT carry the change time (change_ts).
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ecom_lab.silver.customers_scd2 (
  customer_id BIGINT COMMENT 'Unique customer identifier from shop_db',
  name STRING COMMENT 'Customer full name [PII: NAME - MEDIUM]',
  email STRING MASK ecom_lab.ops.mask_email COMMENT 'Customer email, visible in full only to pii_readers [PII: EMAIL - HIGH]',
  phone STRING MASK ecom_lab.ops.mask_phone COMMENT 'Customer phone, last 4 digits for non-pii_readers [PII: PHONE - HIGH]',
  address STRING MASK ecom_lab.ops.mask_address COMMENT 'Customer address, city only for non-pii_readers [PII: ADDRESS - MEDIUM]',
  province STRING COMMENT 'Province code (e.g. ON, BC); row-filter key for regional access (GV-02)',
  loyalty_tier STRING COMMENT 'Loyalty tier: bronze, silver or gold; versioned in SCD2',
  loyalty_points BIGINT COMMENT 'Loyalty points; tracked in place, excluded from history (CDC-07)',
  created_at TIMESTAMP COMMENT 'Customer account creation time',
  email_hash STRING COMMENT 'sha2(email, 256) - join key for analysts without PII exposure (GV-04)',
  data_quality_flag STRING COMMENT 'Row-level data quality status: CLEAN / MISSING_EMAIL / MISSING_PHONE / MISSING_NAME / MISSING_ADDRESS',
  audit_timestamp TIMESTAMP COMMENT 'Pipeline execution timestamp',
  source_system STRING COMMENT 'Upstream source system identifier',
  __START_AT TIMESTAMP COMMENT 'SCD2 validity start: change time (ts_ms) of the version-creating change',
  __END_AT TIMESTAMP COMMENT 'SCD2 validity end: change time of the next version; NULL for the current one'
)
COMMENT 'Cleaned and validated customer dimension from bronze.customers (SCD2) - CONTAINS PII: name, email, phone, address'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain' = 'ecommerce',
  'contains_pii' = 'true',
  'pii_columns' = 'name,email,phone,address',
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY AUTO
WITH ROW FILTER ecom_lab.ops.province_filter ON (province);

CREATE FLOW customers_cdc AS AUTO CDC INTO ecom_lab.silver.customers_scd2
FROM STREAM(customers_flat)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts, _ingest_ts, _source_file)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (loyalty_points);

-- ----------------------------------------------------------------------------
-- products_scd1 (CDC) - SCD TYPE 1
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ecom_lab.silver.products_scd1
COMMENT 'Cleaned and validated product dimension, current state only (SCD1), from bronze.products'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain' = 'ecommerce',
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY AUTO;

CREATE FLOW products_cdc AS AUTO CDC INTO ecom_lab.silver.products_scd1
FROM STREAM(products_flat)
KEYS (product_id)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts, _ingest_ts, _source_file)
STORED AS SCD TYPE 1;

-- ----------------------------------------------------------------------------
-- inventory_scd1 (CDC) - SCD TYPE 1 - key: product_id + warehouse_id
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ecom_lab.silver.inventory_scd1
COMMENT 'Cleaned and validated current stock per product and warehouse (SCD1) from bronze.inventory'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain' = 'ecommerce',
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY AUTO;

CREATE FLOW inventory_cdc AS AUTO CDC INTO ecom_lab.silver.inventory_scd1
FROM STREAM(inventory_flat)
KEYS (product_id, warehouse_id)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts, _ingest_ts, _source_file)
STORED AS SCD TYPE 1;

-- ----------------------------------------------------------------------------
-- orders (CDC) - SCD TYPE 1 (latest status per order_id, CDC-09) - province row
-- filter declared here so it survives a full refresh (GV-02, GV-03)
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ecom_lab.silver.orders
COMMENT 'Cleaned and validated order facts, current state per order_id (SCD1: latest status), from bronze.orders'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain' = 'ecommerce',
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY AUTO
WITH ROW FILTER ecom_lab.ops.province_filter ON (province);

CREATE FLOW orders_cdc AS AUTO CDC INTO ecom_lab.silver.orders
FROM STREAM(orders_flat)
KEYS (order_id)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts, _ingest_ts, _source_file)
STORED AS SCD TYPE 1;

-- ----------------------------------------------------------------------------
-- clicks (events) - append-only, no CDC - CONTAINS PII: ip, customer_id
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ecom_lab.silver.clicks (
  CONSTRAINT valid_event_id EXPECT (event_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_event_type EXPECT (event_type IN ('view', 'add_to_cart', 'checkout')) ON VIOLATION DROP ROW
)
COMMENT 'Cleaned and validated click events from bronze.clicks - CONTAINS PII: ip, customer_id'
TBLPROPERTIES (
  'quality' = 'silver',
  'domain' = 'ecommerce',
  'contains_pii' = 'true',
  'pii_columns' = 'ip,customer_id',
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true'
)
CLUSTER BY AUTO
AS SELECT
  event_id,
  session_id,
  customer_id,                   -- [PII: PSEUDONYMOUS_ID - LOW]
  event_type,
  product_id,
  CAST(event_ts AS TIMESTAMP) AS event_ts,
  device,
  ip,                            -- [PII: IP_ADDRESS - MEDIUM]
  CASE
    WHEN ip IS NULL THEN 'MISSING_IP'
    WHEN device IS NULL THEN 'MISSING_DEVICE'
    ELSE 'CLEAN'
  END AS data_quality_flag,
  current_timestamp() AS audit_timestamp,
  'web_clickstream' AS source_system
FROM STREAM(ecom_lab.bronze.clicks);
