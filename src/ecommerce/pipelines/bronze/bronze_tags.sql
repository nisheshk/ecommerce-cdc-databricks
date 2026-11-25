-- Bronze column descriptions and Unity Catalog tags.
-- NOT a pipeline library: pipeline source may only contain dataset
-- definitions, so this metadata DDL runs separately, after the bronze tables
-- exist (i.e. after the first successful ecom_pipeline update). Idempotent:
-- re-run it after any full refresh of the bronze tables, which re-creates them
-- and drops comments and tags (RR-03, AB-04).
-- Governed 'pii' tag allowed values: ssn, phone, age, date_of_birth, salary,
-- address, name. 'false' is not allowed, so PII-free tables use the informal
-- 'pii_type' = 'none' tag instead; clicks uses 'pii_type' for ip/customer_id,
-- which are not in the policy's value list.

-- ----------------------------------------------------------------------------
-- customers - CONTAINS PII: name, email, phone, address
-- ----------------------------------------------------------------------------
ALTER TABLE ecom_lab.bronze.customers ALTER COLUMN op COMMENT 'Debezium operation: c = create, u = update, d = delete, r = initial snapshot read';
ALTER TABLE ecom_lab.bronze.customers ALTER COLUMN ts_ms COMMENT 'Source database commit time in epoch milliseconds; never used for SEQUENCE BY (use source.lsn)';
ALTER TABLE ecom_lab.bronze.customers ALTER COLUMN source COMMENT 'Debezium source metadata struct; source.lsn is the strictly increasing log sequence number';
ALTER TABLE ecom_lab.bronze.customers ALTER COLUMN before COMMENT 'Row image before the change; the row values when op = d - CONTAINS PII: name, email, phone, address';
ALTER TABLE ecom_lab.bronze.customers ALTER COLUMN after COMMENT 'Row image after the change - CONTAINS PII: name, email, phone, address';
ALTER TABLE ecom_lab.bronze.customers ALTER COLUMN _ingest_ts COMMENT 'Landing file modification time captured by Auto Loader';
ALTER TABLE ecom_lab.bronze.customers ALTER COLUMN _source_file COMMENT 'Landing file path captured by Auto Loader';
ALTER TABLE ecom_lab.bronze.customers ALTER COLUMN audit_timestamp COMMENT 'Pipeline execution timestamp';
ALTER TABLE ecom_lab.bronze.customers ALTER COLUMN source_system COMMENT 'Upstream source system identifier';

ALTER TABLE ecom_lab.bronze.customers
  SET TAGS ('quality' = 'bronze', 'domain' = 'ecommerce', 'pii' = 'name',
            'pii_type' = 'name,email,phone,address', 'data_classification' = 'confidential');

-- ----------------------------------------------------------------------------
-- products
-- ----------------------------------------------------------------------------
ALTER TABLE ecom_lab.bronze.products ALTER COLUMN op COMMENT 'Debezium operation: c = create, u = update, d = delete, r = initial snapshot read';
ALTER TABLE ecom_lab.bronze.products ALTER COLUMN ts_ms COMMENT 'Source database commit time in epoch milliseconds; never used for SEQUENCE BY (use source.lsn)';
ALTER TABLE ecom_lab.bronze.products ALTER COLUMN source COMMENT 'Debezium source metadata struct; source.lsn is the strictly increasing log sequence number';
ALTER TABLE ecom_lab.bronze.products ALTER COLUMN before COMMENT 'Row image before the change; the row values when op = d';
ALTER TABLE ecom_lab.bronze.products ALTER COLUMN after COMMENT 'Row image after the change: product_id, name, category, price, is_active';
ALTER TABLE ecom_lab.bronze.products ALTER COLUMN _ingest_ts COMMENT 'Landing file modification time captured by Auto Loader';
ALTER TABLE ecom_lab.bronze.products ALTER COLUMN _source_file COMMENT 'Landing file path captured by Auto Loader';
ALTER TABLE ecom_lab.bronze.products ALTER COLUMN audit_timestamp COMMENT 'Pipeline execution timestamp';
ALTER TABLE ecom_lab.bronze.products ALTER COLUMN source_system COMMENT 'Upstream source system identifier';

ALTER TABLE ecom_lab.bronze.products
  SET TAGS ('quality' = 'bronze', 'domain' = 'ecommerce', 'pii_type' = 'none',
            'data_classification' = 'internal');

-- ----------------------------------------------------------------------------
-- inventory
-- ----------------------------------------------------------------------------
ALTER TABLE ecom_lab.bronze.inventory ALTER COLUMN op COMMENT 'Debezium operation: c = create, u = update, d = delete, r = initial snapshot read';
ALTER TABLE ecom_lab.bronze.inventory ALTER COLUMN ts_ms COMMENT 'Source database commit time in epoch milliseconds; never used for SEQUENCE BY (use source.lsn)';
ALTER TABLE ecom_lab.bronze.inventory ALTER COLUMN source COMMENT 'Debezium source metadata struct; source.lsn is the strictly increasing log sequence number';
ALTER TABLE ecom_lab.bronze.inventory ALTER COLUMN before COMMENT 'Row image before the change; the row values when op = d';
ALTER TABLE ecom_lab.bronze.inventory ALTER COLUMN after COMMENT 'Row image after the change: product_id, warehouse_id, qty_on_hand, updated_at';
ALTER TABLE ecom_lab.bronze.inventory ALTER COLUMN _ingest_ts COMMENT 'Landing file modification time captured by Auto Loader';
ALTER TABLE ecom_lab.bronze.inventory ALTER COLUMN _source_file COMMENT 'Landing file path captured by Auto Loader';
ALTER TABLE ecom_lab.bronze.inventory ALTER COLUMN audit_timestamp COMMENT 'Pipeline execution timestamp';
ALTER TABLE ecom_lab.bronze.inventory ALTER COLUMN source_system COMMENT 'Upstream source system identifier';

ALTER TABLE ecom_lab.bronze.inventory
  SET TAGS ('quality' = 'bronze', 'domain' = 'ecommerce', 'pii_type' = 'none',
            'data_classification' = 'internal');

-- ----------------------------------------------------------------------------
-- orders
-- ----------------------------------------------------------------------------
ALTER TABLE ecom_lab.bronze.orders ALTER COLUMN op COMMENT 'Debezium operation: c = create, u = update, d = delete, r = initial snapshot read';
ALTER TABLE ecom_lab.bronze.orders ALTER COLUMN ts_ms COMMENT 'Source database commit time in epoch milliseconds; never used for SEQUENCE BY (use source.lsn)';
ALTER TABLE ecom_lab.bronze.orders ALTER COLUMN source COMMENT 'Debezium source metadata struct; source.lsn is the strictly increasing log sequence number';
ALTER TABLE ecom_lab.bronze.orders ALTER COLUMN before COMMENT 'Row image before the change; the row values when op = d';
ALTER TABLE ecom_lab.bronze.orders ALTER COLUMN after COMMENT 'Row image after the change: order_id, customer_id, items, total, status, order_ts, province';
ALTER TABLE ecom_lab.bronze.orders ALTER COLUMN _ingest_ts COMMENT 'Landing file modification time captured by Auto Loader';
ALTER TABLE ecom_lab.bronze.orders ALTER COLUMN _source_file COMMENT 'Landing file path captured by Auto Loader';
ALTER TABLE ecom_lab.bronze.orders ALTER COLUMN audit_timestamp COMMENT 'Pipeline execution timestamp';
ALTER TABLE ecom_lab.bronze.orders ALTER COLUMN source_system COMMENT 'Upstream source system identifier';

ALTER TABLE ecom_lab.bronze.orders
  SET TAGS ('quality' = 'bronze', 'domain' = 'ecommerce', 'pii_type' = 'none',
            'data_classification' = 'internal');

-- ----------------------------------------------------------------------------
-- clicks - CONTAINS PII: ip, customer_id
-- ----------------------------------------------------------------------------
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN event_id COMMENT 'Unique click event identifier from the web clickstream';
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN session_id COMMENT 'Browser session the click belongs to';
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN customer_id COMMENT 'References ecom_lab.silver.customers_scd2.customer_id; nullable for anonymous traffic [PII: PSEUDONYMOUS_ID - LOW]';
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN event_type COMMENT 'Funnel step: view, add_to_cart or checkout';
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN product_id COMMENT 'References ecom_lab.silver.products_scd1.product_id';
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN event_ts COMMENT 'Click time from the client device';
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN device COMMENT 'Device type: mobile, desktop or tablet';
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN ip COMMENT 'Client IP address [PII: IP_ADDRESS - MEDIUM]';
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN _ingest_ts COMMENT 'Landing file modification time captured by Auto Loader';
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN _source_file COMMENT 'Landing file path captured by Auto Loader';
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN audit_timestamp COMMENT 'Pipeline execution timestamp';
ALTER TABLE ecom_lab.bronze.clicks ALTER COLUMN source_system COMMENT 'Upstream source system identifier';

ALTER TABLE ecom_lab.bronze.clicks
  SET TAGS ('quality' = 'bronze', 'domain' = 'ecommerce', 'pii_type' = 'ip_address,pseudonymous_id',
            'data_classification' = 'confidential');