# Naming reference

## Catalog and schemas

| Object | Name | Holds |
|--------|------|-------|
| Catalog | `ecom_lab` | Everything for the lab |
| Volume | `ecom_lab.landing.raw` | Generator files: `cdc/<feed>/`, `events/clicks/` |
| Schema | `ecom_lab.bronze` | Raw append-only streaming tables, one per feed |
| Schema | `ecom_lab.silver` | Cleaned, deduped, CDC applied, joined |
| Schema | `ecom_lab.gold` | Aggregates and KPIs, no PII |
| Schema | `ecom_lab.ops` | Quarantine, orphans, test results, policy functions |

## Tables

- `lowercase_snake_case`, plural entity name: `customers`, `orders`, `clicks`.
- No layer prefix: the schema carries the layer (`ecom_lab.silver.orders`, not `ecom_lab.silver.silver_orders`).
- CDC targets end in their SCD type: `customers_scd1`, `customers_scd2`, `products_scd1`.
- Joined tables name both sides, fact first: `click_to_order`.
- Gold names the measure and grain: `revenue_5min`, `funnel_daily`, `customer_ltv`.
- Quarantine tables name what they hold: `ops.orphan_orders`, `ops.bad_customers`.
- Views used only inside a pipeline end in `_flat` or `_clean`: `customers_flat`.

## Columns

- `lowercase_snake_case`. Keys are `<entity>_id`.
- Timestamps end in `_ts` (`order_ts`, `event_ts`) or `_at` (`created_at`, `updated_at`).
- Booleans start with `is_` or describe a state: `is_active`, `marketing_opt_in`.
- Masked or derived PII says so: `email_hash`, `phone_masked`.
- Pipeline metadata: `_ingest_ts`, `_source_file` (bronze), then `audit_timestamp`, `source_system` (last, every table).
