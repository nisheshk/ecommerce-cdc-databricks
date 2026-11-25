-- Silver column descriptions and Unity Catalog tags.
-- NOT a pipeline library: pipeline source may only contain dataset
-- definitions, so this metadata DDL runs separately, after the silver tables
-- exist (i.e. after a successful ecom_pipeline update). Idempotent: re-run it
-- after any full refresh of the silver tables, which re-creates them and
-- drops comments and tags (RR-03, GV-03 policies themselves survive because
-- they are declared in silver.sql's CREATE statements).
-- Governed 'pii' tag allowed values: ssn, phone, age, date_of_birth, salary,
-- address, name. 'false' is not allowed, so PII-free tables use the informal
-- 'pii_type' = 'none' tag instead; clicks and the quarantine record use
-- 'pii_type' for ip/customer_id/record, which are not in the policy's list.

-- ----------------------------------------------------------------------------
-- customers_scd2 - CONTAINS PII: name, email, phone, address
-- (column comments already declared inline in silver.sql's CREATE schema)
-- ----------------------------------------------------------------------------
ALTER TABLE ecom_lab.silver.customers_scd2
  SET TAGS ('quality' = 'silver', 'domain' = 'ecommerce', 'pii' = 'name',
            'pii_type' = 'name,email,phone,address', 'data_classification' = 'confidential');

-- ----------------------------------------------------------------------------
-- products_scd1
-- ----------------------------------------------------------------------------
ALTER TABLE ecom_lab.silver.products_scd1 ALTER COLUMN product_id COMMENT 'Unique product identifier from shop_db';
ALTER TABLE ecom_lab.silver.products_scd1 ALTER COLUMN price COMMENT 'Current product price in CAD; latest value only, SCD1 overwrites (RR-10 diff for price history)';
ALTER TABLE ecom_lab.silver.products_scd1 ALTER COLUMN is_active COMMENT 'Whether the product is currently sellable; discontinued products end with is_active = false';
ALTER TABLE ecom_lab.silver.products_scd1 ALTER COLUMN data_quality_flag COMMENT 'Row-level data quality status: CLEAN / MISSING_NAME / MISSING_PRICE / NEGATIVE_PRICE';
ALTER TABLE ecom_lab.silver.products_scd1 ALTER COLUMN audit_timestamp COMMENT 'Pipeline execution timestamp';
ALTER TABLE ecom_lab.silver.products_scd1 ALTER COLUMN source_system COMMENT 'Upstream source system identifier';

ALTER TABLE ecom_lab.silver.products_scd1
  SET TAGS ('quality' = 'silver', 'domain' = 'ecommerce', 'pii_type' = 'none',
            'data_classification' = 'internal');

-- ----------------------------------------------------------------------------
-- inventory_scd1
-- ----------------------------------------------------------------------------
ALTER TABLE ecom_lab.silver.inventory_scd1 ALTER COLUMN product_id COMMENT 'Product identifier; part of the composite key with warehouse_id';
ALTER TABLE ecom_lab.silver.inventory_scd1 ALTER COLUMN warehouse_id COMMENT 'Warehouse identifier; part of the composite key with product_id';
ALTER TABLE ecom_lab.silver.inventory_scd1 ALTER COLUMN qty_on_hand COMMENT 'Current quantity on hand; SCD1 keeps the latest per key';
ALTER TABLE ecom_lab.silver.inventory_scd1 ALTER COLUMN updated_at COMMENT 'Source-side last update time of the stock level';
ALTER TABLE ecom_lab.silver.inventory_scd1 ALTER COLUMN data_quality_flag COMMENT 'Row-level data quality status: CLEAN / MISSING_QTY / NEGATIVE_QTY';
ALTER TABLE ecom_lab.silver.inventory_scd1 ALTER COLUMN audit_timestamp COMMENT 'Pipeline execution timestamp';
ALTER TABLE ecom_lab.silver.inventory_scd1 ALTER COLUMN source_system COMMENT 'Upstream source system identifier';

ALTER TABLE ecom_lab.silver.inventory_scd1
  SET TAGS ('quality' = 'silver', 'domain' = 'ecommerce', 'pii_type' = 'none',
            'data_classification' = 'internal');

-- ----------------------------------------------------------------------------
-- orders
-- ----------------------------------------------------------------------------
ALTER TABLE ecom_lab.silver.orders ALTER COLUMN order_id COMMENT 'Unique order identifier from shop_db';
ALTER TABLE ecom_lab.silver.orders ALTER COLUMN customer_id COMMENT 'References ecom_lab.silver.customers_scd2.customer_id; can be null on bad records before validation';
ALTER TABLE ecom_lab.silver.orders ALTER COLUMN items COMMENT 'Order line items: array of struct(product_id, qty, unit_price)';
ALTER TABLE ecom_lab.silver.orders ALTER COLUMN total COMMENT 'Order total in CAD; latest value per order (SCD1 status lifecycle, CDC-09)';
ALTER TABLE ecom_lab.silver.orders ALTER COLUMN status COMMENT 'Order status: placed, paid, shipped, delivered, cancelled or refunded; latest wins (CDC-09)';
ALTER TABLE ecom_lab.silver.orders ALTER COLUMN order_ts COMMENT 'Order placement time; event-time column for gold watermarks (LA-01 to LA-08)';
ALTER TABLE ecom_lab.silver.orders ALTER COLUMN province COMMENT 'Fulfillment province code; row-filter key for regional access (GV-02)';
ALTER TABLE ecom_lab.silver.orders ALTER COLUMN data_quality_flag COMMENT 'Row-level data quality status: CLEAN / MISSING_TOTAL / NEGATIVE_TOTAL / MISSING_CUSTOMER';
ALTER TABLE ecom_lab.silver.orders ALTER COLUMN audit_timestamp COMMENT 'Pipeline execution timestamp';
ALTER TABLE ecom_lab.silver.orders ALTER COLUMN source_system COMMENT 'Upstream source system identifier';

ALTER TABLE ecom_lab.silver.orders
  SET TAGS ('quality' = 'silver', 'domain' = 'ecommerce', 'pii_type' = 'none',
            'data_classification' = 'internal');

-- ----------------------------------------------------------------------------
-- clicks - CONTAINS PII: ip, customer_id
-- ----------------------------------------------------------------------------
ALTER TABLE ecom_lab.silver.clicks ALTER COLUMN event_id COMMENT 'Unique click event identifier from the web clickstream';
ALTER TABLE ecom_lab.silver.clicks ALTER COLUMN session_id COMMENT 'Browser session the click belongs to';
ALTER TABLE ecom_lab.silver.clicks ALTER COLUMN customer_id COMMENT 'References ecom_lab.silver.customers_scd2.customer_id; nullable for anonymous traffic [PII: PSEUDONYMOUS_ID - LOW]';
ALTER TABLE ecom_lab.silver.clicks ALTER COLUMN event_type COMMENT 'Funnel step: view, add_to_cart or checkout';
ALTER TABLE ecom_lab.silver.clicks ALTER COLUMN product_id COMMENT 'References ecom_lab.silver.products_scd1.product_id';
ALTER TABLE ecom_lab.silver.clicks ALTER COLUMN event_ts COMMENT 'Click time from the client device; event-time column for stream-stream joins (JN-06)';
ALTER TABLE ecom_lab.silver.clicks ALTER COLUMN device COMMENT 'Device type: mobile, desktop or tablet';
ALTER TABLE ecom_lab.silver.clicks ALTER COLUMN ip COMMENT 'Client IP address [PII: IP_ADDRESS - MEDIUM]';
ALTER TABLE ecom_lab.silver.clicks ALTER COLUMN data_quality_flag COMMENT 'Row-level data quality status: CLEAN / MISSING_IP / MISSING_DEVICE';
ALTER TABLE ecom_lab.silver.clicks ALTER COLUMN audit_timestamp COMMENT 'Pipeline execution timestamp';
ALTER TABLE ecom_lab.silver.clicks ALTER COLUMN source_system COMMENT 'Upstream source system identifier';

ALTER TABLE ecom_lab.silver.clicks
  SET TAGS ('quality' = 'silver', 'domain' = 'ecommerce', 'pii_type' = 'ip_address,pseudonymous_id',
            'data_classification' = 'confidential');

-- ----------------------------------------------------------------------------
-- ops.cdc_quarantine - CONTAINS PII: record (JSON of source row)
-- (column comments already declared inline in silver.sql's CREATE schema)
-- ----------------------------------------------------------------------------
ALTER TABLE ecom_lab.ops.cdc_quarantine
  SET TAGS ('quality' = 'silver', 'domain' = 'ecommerce', 'pii_type' = 'record',
            'data_classification' = 'confidential');
