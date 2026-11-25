-- ============================================================================
-- Bronze layer (SPEC.md build order step 2): raw, append-only ingestion of all
-- five feeds from /Volumes/ecom_lab/landing/raw.
-- CDC feeds keep the raw Debezium envelope (op, ts_ms, source, before, after);
-- flattening happens in silver views, never here. Clicks land as plain events.
-- Schema evolution: default addNewColumns keeps history safe in _rescued_data
-- (SE-01 to SE-03). Bronze uses WHERE-free SELECT *: nothing is filtered out.
--
-- UC tags and column descriptions: pipeline source may only contain dataset
-- definitions, so ALTER TABLE ... COMMENT / SET TAGS statements live in
-- src/ecommerce/governance/policies.sql and are applied after the tables exist.
-- Tag values there respect the governed 'pii' policy (allowed: ssn, phone, age,
-- date_of_birth, salary, address, name): 'false' is not an allowed value, so
-- PII-free tables carry the informal 'pii_type' = 'none' tag instead of a
-- governed 'pii' value, and clicks uses 'pii_type' for ip/customer_id, which
-- are not in the policy's value list.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- customers (CDC) - CONTAINS PII: name, email, phone, address
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ecom_lab.bronze.customers
COMMENT "Raw customer CDC events ingested from /Volumes/ecom_lab/landing/raw/cdc/customers - CONTAINS PII: name, email, phone, address"
TBLPROPERTIES (
  "quality" = "bronze",
  "domain" = "ecommerce",
  "contains_pii" = "true",
  "pii_columns" = "name,email,phone,address",
  "delta.enableRowTracking" = "true"
)
CLUSTER BY AUTO
AS SELECT
  *,                     -- before/after hold [PII: NAME - MEDIUM], [PII: EMAIL - HIGH], [PII: PHONE - HIGH], [PII: ADDRESS - MEDIUM]
  _metadata.file_modification_time AS _ingest_ts,
  _metadata.file_path AS _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM read_files(
  "/Volumes/ecom_lab/landing/raw/cdc/customers/",
  format => "json"
);


-- ----------------------------------------------------------------------------
-- products (CDC)
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ecom_lab.bronze.products
COMMENT "Raw product CDC events ingested from /Volumes/ecom_lab/landing/raw/cdc/products"
TBLPROPERTIES (
  "quality" = "bronze",
  "domain" = "ecommerce",
  "delta.enableRowTracking" = "true"
)
CLUSTER BY AUTO
AS SELECT
  *,
  _metadata.file_modification_time AS _ingest_ts,
  _metadata.file_path AS _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM read_files(
  "/Volumes/ecom_lab/landing/raw/cdc/products/",
  format => "json"
);


-- ----------------------------------------------------------------------------
-- inventory (CDC) - key: product_id + warehouse_id
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ecom_lab.bronze.inventory
COMMENT "Raw inventory CDC events ingested from /Volumes/ecom_lab/landing/raw/cdc/inventory"
TBLPROPERTIES (
  "quality" = "bronze",
  "domain" = "ecommerce",
  "delta.enableRowTracking" = "true"
)
CLUSTER BY AUTO
AS SELECT
  *,
  _metadata.file_modification_time AS _ingest_ts,
  _metadata.file_path AS _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM read_files(
  "/Volumes/ecom_lab/landing/raw/cdc/inventory/",
  format => "json"
);


-- ----------------------------------------------------------------------------
-- orders (CDC) - no PII itself, joins to PII in customers
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ecom_lab.bronze.orders
COMMENT "Raw order CDC events ingested from /Volumes/ecom_lab/landing/raw/cdc/orders; joins to PII in customers"
TBLPROPERTIES (
  "quality" = "bronze",
  "domain" = "ecommerce",
  "delta.enableRowTracking" = "true"
)
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


-- ----------------------------------------------------------------------------
-- clicks (events) - CONTAINS PII: ip, customer_id
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE ecom_lab.bronze.clicks
COMMENT "Raw click events ingested from /Volumes/ecom_lab/landing/raw/events/clicks - CONTAINS PII: ip, customer_id"
TBLPROPERTIES (
  "quality" = "bronze",
  "domain" = "ecommerce",
  "contains_pii" = "true",
  "pii_columns" = "ip,customer_id",
  "delta.enableRowTracking" = "true"
)
CLUSTER BY AUTO
AS SELECT
  *,                     -- * includes [PII: IP_ADDRESS - MEDIUM] ip and [PII: PSEUDONYMOUS_ID - LOW] customer_id
  _metadata.file_modification_time AS _ingest_ts,
  _metadata.file_path AS _source_file,
  current_timestamp() AS audit_timestamp,
  'web_clickstream' AS source_system
FROM STREAM read_files(
  "/Volumes/ecom_lab/landing/raw/events/clicks/",
  format => "json"
);
