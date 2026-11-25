-- Governance policies for the e-commerce CDC lab (SPEC.md GV-01 to GV-07).
-- One policy plane in ecom_lab.ops so every reader sees the same rules.
--
-- Run order:
--   1. this file -- functions + access table; idempotent, run before silver
--   2. pipeline updates -- silver CREATEs reference these functions via
--      MASK / WITH ROW FILTER so the policies survive full refreshes (GV-03)
--   3. pipelines/<layer>/<layer>_tags.sql -- column comments and UC tags,
--      run after the tables exist and re-run after any full refresh
--
-- How to attach a policy to a table:
--   - Pipeline-managed tables (preferred): declare it in the pipeline CREATE
--       CREATE OR REFRESH STREAMING TABLE ecom_lab.silver.customers_scd2 (
--         email STRING MASK ecom_lab.ops.mask_email,
--         phone STRING MASK ecom_lab.ops.mask_phone
--       ) WITH ROW FILTER ecom_lab.ops.province_filter ON (province) ...
--     The pipeline owns the table DDL, so the policy is re-applied on every
--     full refresh (GV-03). ALTER TABLE ... SET MASK on a pipeline table would
--     be lost when a full refresh re-creates it.
--   - Plain Delta tables: ALTER TABLE ... ALTER COLUMN ... SET MASK and
--     ALTER TABLE ... SET ROW FILTER ... ON (...).

-- ----------------------------------------------------------------------------
-- Access mapping table for the row filter (GV-02; Free Edition-friendly)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ecom_lab.ops.user_region_access (
  user_email STRING COMMENT 'Workspace user email; must match current_user()',
  province STRING COMMENT 'Province this user may read; ALL = every province'
)
COMMENT 'Maps users to provinces they may see; input to ecom_lab.ops.province_filter';

-- ----------------------------------------------------------------------------
-- Row filter: who sees which province (GV-02)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ecom_lab.ops.province_filter(p STRING)
  RETURN EXISTS (SELECT 1 FROM ecom_lab.ops.user_region_access a
                 WHERE a.user_email = current_user()
                   AND (a.province = p OR a.province = 'ALL'));

-- ----------------------------------------------------------------------------
-- Column masks: full value for pii_readers, masked for everyone else (GV-01)
-- Same shape would apply to an ssn column: keep last 4 digits only, e.g.
--   CONCAT('***-**-', RIGHT(REGEXP_REPLACE(s, '[^0-9]', ''), 4))
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ecom_lab.ops.mask_email(e STRING)
  RETURN CASE WHEN is_account_group_member('pii_readers') THEN e
              ELSE CONCAT('***@', SPLIT_PART(e, '@', 2)) END;

CREATE OR REPLACE FUNCTION ecom_lab.ops.mask_phone(p STRING)
  RETURN CASE WHEN is_account_group_member('pii_readers') THEN p
              ELSE CONCAT('***-***-', RIGHT(REGEXP_REPLACE(p, '[^0-9]', ''), 4)) END;

CREATE OR REPLACE FUNCTION ecom_lab.ops.mask_address(a STRING)
  RETURN CASE WHEN is_account_group_member('pii_readers') THEN a
              ELSE TRIM(SPLIT(a, ',')[1]) END;  -- city only from "123 King St, Toronto, ON"

-- ----------------------------------------------------------------------------
-- Grants (GV-06). Fill in the real role/user names, then uncomment:
-- ----------------------------------------------------------------------------
-- GRANT USE CATALOG ON CATALOG ecom_lab TO `analysts`;
-- GRANT USE SCHEMA ON SCHEMA ecom_lab.gold TO `analysts`;
-- GRANT SELECT ON SCHEMA ecom_lab.gold TO `analysts`;  -- gold only: no bronze/silver
