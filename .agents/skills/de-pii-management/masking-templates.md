<!-- Adapted from databricks-solutions/genie-code-skills-demo skills/pii-management/masking-templates.md
     (DB license, see THIRD_PARTY_NOTICES.md). Changed: e-commerce tables, ops policy functions, forget-me. -->

# PII masking templates

Reusable SQL for `@de-pii-management`. Substitute source tables and column names.

## Bronze -- pass through with markers

```sql
CREATE OR REFRESH STREAMING TABLE ecom_lab.bronze.customers
COMMENT "Raw customer CDC events from /Volumes/ecom_lab/landing/raw/cdc/customers - CONTAINS PII: name, email, phone, address"
TBLPROPERTIES (
  "quality" = "bronze",
  "domain" = "ecommerce",
  "contains_pii" = "true",
  "pii_columns" = "name,email,phone,address"
)
CLUSTER BY AUTO
AS SELECT
  *,                     -- before/after hold [PII: NAME, EMAIL, PHONE, ADDRESS]
  _metadata.file_modification_time AS _ingest_ts,
  _metadata.file_path AS _source_file,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM STREAM read_files("/Volumes/ecom_lab/landing/raw/cdc/customers/", format => "json");
```

## Silver -- mask and derive

```sql
-- Hash email for matching without exposing raw value
SHA2(LOWER(TRIM(email)), 256) AS email_hash,

-- Mask phone - show last 4 digits only
CONCAT('***-***-', RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 4)) AS phone_masked,

-- City only from "123 King St, Toronto, ON"
TRIM(SPLIT(address, ',')[1]) AS city,

-- Truncate IP to /24 for clickstream analytics
REGEXP_REPLACE(ip, '\\.[0-9]+$', '.0') AS ip_prefix
```

## Silver -- Unity Catalog masks and row filter

```sql
-- Who sees which province (works without account groups on Free Edition)
CREATE TABLE IF NOT EXISTS ecom_lab.ops.user_region_access (user_email STRING, province STRING);

CREATE OR REPLACE FUNCTION ecom_lab.ops.province_filter(p STRING) RETURN
  EXISTS (SELECT 1 FROM ecom_lab.ops.user_region_access
          WHERE user_email = current_user() AND (province = p OR province = 'ALL'));

CREATE OR REPLACE FUNCTION ecom_lab.ops.mask_email(e STRING) RETURN
  CASE WHEN is_account_group_member('pii_readers') THEN e
       ELSE CONCAT('***@', SPLIT_PART(e, '@', 2)) END;

CREATE OR REPLACE FUNCTION ecom_lab.ops.mask_phone(p STRING) RETURN
  CASE WHEN is_account_group_member('pii_readers') THEN p
       ELSE CONCAT('***-***-', RIGHT(REGEXP_REPLACE(p, '[^0-9]', ''), 4)) END;

-- Declared on the pipeline table so the policy survives a full refresh
CREATE OR REFRESH STREAMING TABLE ecom_lab.silver.customers_scd2 (
  email STRING MASK ecom_lab.ops.mask_email,
  phone STRING MASK ecom_lab.ops.mask_phone
)
WITH ROW FILTER ecom_lab.ops.province_filter ON (province)
COMMENT "Customer history (SCD2) from bronze.customers via AUTO CDC - CONTAINS PII: name, email, phone, address"
TBLPROPERTIES ("quality" = "silver", "domain" = "ecommerce", "contains_pii" = "true",
               "pii_columns" = "name,email,phone,address");
```

## Gold -- aggregated only (no individual PII)

```sql
CREATE OR REFRESH MATERIALIZED VIEW ecom_lab.gold.customer_segments
COMMENT "Customer counts by province and loyalty tier - NO PII"
TBLPROPERTIES ("quality" = "gold", "domain" = "ecommerce")
AS SELECT
  province,
  loyalty_tier,
  COUNT(DISTINCT customer_id) AS customer_count,
  current_timestamp() AS audit_timestamp,
  'gold_aggregation' AS source_system
FROM ecom_lab.silver.customers_scd2
WHERE __END_AT IS NULL
GROUP BY province, loyalty_tier;
```

## Right to be forgotten

```sql
DELETE FROM ecom_lab.bronze.customers
WHERE COALESCE(after.customer_id, before.customer_id) = 42;
-- Full refresh silver.customers_scd2 to rebuild history without the person, then:
VACUUM ecom_lab.bronze.customers;
```
