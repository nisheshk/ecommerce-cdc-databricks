---
name: de-pii-management
description: Identify, label and protect PII columns in pipeline tables. Use when creating or modifying tables that contain customer or personal data, such as customers or clickstream IPs. Includes detection patterns, table property labeling, column annotation, Unity Catalog masks and row filters, and right-to-be-forgotten deletes.
---

<!-- Adapted from databricks-solutions/genie-code-skills-demo skills/pii-management (DB license, see
     THIRD_PARTY_NOTICES.md). Changed: skill names, IP and customer_id rows, UC-mask silver rule, forget-me. -->

# PII Management in SDP Pipelines

When a table contains personal or sensitive data, apply these detection, labelling, and protection rules.

For bronze/silver/gold SQL and Unity Catalog masking functions, read [masking-templates.md](masking-templates.md).

## Identify PII Columns

Flag columns matching these patterns as PII:

| Column Pattern | PII Type | Risk |
|----------------|----------|------|
| `email`, `email_address` | EMAIL | HIGH |
| `phone`, `mobile`, `telephone` | PHONE | HIGH |
| `name`, `first_name`, `last_name`, `full_name` | NAME | MEDIUM |
| `date_of_birth`, `dob` | DOB | MEDIUM |
| `address`, `street`, `postal_code` | ADDRESS | MEDIUM |
| `ip`, `ip_address` | IP_ADDRESS | MEDIUM |
| `customer_id` on behavioural data (clicks) | PSEUDONYMOUS_ID | LOW |
| `ssn`, `national_id`, `tax_id` | SSN | CRITICAL |
| `account_number`, `iban`, `sort_code` | ACCOUNT | HIGH |
| `credit_score`, `income`, `salary` | FINANCIAL | HIGH |
| `card_number`, `cvv` | PAYMENT | CRITICAL |

In this lab: `customers` (name, email, phone, address) and `clicks` (ip, customer_id).

## Label PII Tables

Any table containing PII columns MUST include these table properties:

```sql
TBLPROPERTIES (
  "quality" = "bronze",
  "domain" = "ecommerce",
  "contains_pii" = "true",
  "pii_columns" = "name,email,phone,address"
)
```

The COMMENT clause MUST mention that the table contains PII:

```sql
COMMENT "Raw customer CDC events - CONTAINS PII: name, email, phone, address"
```

## Annotate PII Columns in SQL

Add inline comments next to PII columns to flag their risk level:

```sql
SELECT
  customer_id,
  name,                  -- [PII: NAME - MEDIUM]
  email,                 -- [PII: EMAIL - HIGH]
  phone,                 -- [PII: PHONE - HIGH]
  address,               -- [PII: ADDRESS - MEDIUM]
  province,
  current_timestamp() AS audit_timestamp,
  'shop_db_cdc' AS source_system
FROM customers_flat;
```

## PII Handling by Layer

| Layer | Rule |
|-------|------|
| Bronze | Pass PII through with markers (properties, COMMENT, inline `-- [PII: ...]` annotations). Readable by the pipeline and admins only. |
| Silver | Keep raw PII only behind Unity Catalog column masks (and a row filter where access is regional). Add `email_hash` for joins. Otherwise replace raw PII with derived or masked equivalents (`phone_masked`, city only). |
| Gold | Aggregated data only -- no individual PII, no masks needed |

Policies are Unity Catalog SQL functions in `ecom_lab.ops`, so every reader sees the same rules. Declare them in the pipeline's `CREATE` statement (`WITH ROW FILTER`, `MASK`) so they survive a full refresh.

## Right to Be Forgotten

A CDC delete removes the current row only. To forget a person, also delete them from bronze and every SCD2 version, then `VACUUM` so old files and time travel no longer hold their data.

Copy the layer-specific SQL from [masking-templates.md](masking-templates.md).
