<!-- Adapted from databricks-solutions/genie-code-skills-demo skills/table-governance/governance-template.md
     (DB license, see THIRD_PARTY_NOTICES.md). Changed: e-commerce customer example on an AUTO CDC target. -->

# Governance templates

Reusable CREATE / ALTER / TAG statements for `@de-table-governance`. Substitute catalog and column names. The layer is the schema (`ecom_lab.bronze`, `ecom_lab.silver`, `ecom_lab.gold`).

## Column comments

```sql
ALTER TABLE ecom_lab.silver.customers_scd2
  ALTER COLUMN customer_id COMMENT 'Unique customer identifier from the shop database',
  ALTER COLUMN email COMMENT 'Customer email [PII: EMAIL - HIGH]; masked by ecom_lab.ops.mask_email',
  ALTER COLUMN email_hash COMMENT 'SHA-256 hash of lowercase trimmed email for matching without PII exposure',
  ALTER COLUMN __START_AT COMMENT 'Change time (ts_ms) at which this version became current (SCD2)',
  ALTER COLUMN __END_AT COMMENT 'Change time at which this version was superseded; NULL for the current version',
  ALTER COLUMN audit_timestamp COMMENT 'Pipeline execution timestamp',
  ALTER COLUMN source_system COMMENT 'Upstream source system identifier';
```

## Unity Catalog tags

```sql
ALTER TABLE ecom_lab.bronze.customers
  SET TAGS ('quality' = 'bronze', 'domain' = 'ecommerce', 'pii' = 'true', 'data_classification' = 'confidential');
```

## Full silver CDC table with governance applied

The flatten view adds `email_hash` and the audit columns, so they flow into the AUTO CDC target as its last columns.

```sql
CREATE OR REFRESH STREAMING TABLE ecom_lab.silver.customers_scd2
COMMENT "Customer history (SCD2) from bronze.customers via AUTO CDC - CONTAINS PII: name, email, phone, address"
TBLPROPERTIES (
  "quality" = "silver",
  "domain" = "ecommerce",
  "contains_pii" = "true",
  "pii_columns" = "name,email,phone,address",
  "delta.enableChangeDataFeed" = "true",
  "delta.enableRowTracking" = "true"
);

CREATE FLOW customers_cdc AS AUTO CDC INTO ecom_lab.silver.customers_scd2
FROM STREAM(customers_flat)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts, _ingest_ts, _source_file)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (loyalty_points, audit_timestamp);

ALTER TABLE ecom_lab.silver.customers_scd2
  ALTER COLUMN customer_id COMMENT 'Unique customer identifier from the shop database',
  ALTER COLUMN email_hash COMMENT 'SHA-256 hash of lowercase trimmed email for matching without PII exposure',
  ALTER COLUMN loyalty_points COMMENT 'Current points balance; updated in place, no history kept';

ALTER TABLE ecom_lab.silver.customers_scd2
  SET TAGS ('quality' = 'silver', 'domain' = 'ecommerce', 'pii' = 'true', 'data_classification' = 'confidential');
```
