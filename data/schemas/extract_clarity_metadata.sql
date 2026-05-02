-- Epic Clarity metadata extract for Tool 3 (business_logic_extractor).
--
-- Pulls table + column descriptions for the tables/views your views
-- reference, with two layers:
--   Layer A: Epic Clarity descriptions (rich -- TABLE_INTRODUCTION,
--            column DESCRIPTION, INI/Item codes) for tables present in
--            CLARITY_TBL.
--   Layer B: SQL Server system catalog fallback (sys.objects, sys.columns)
--            for tables/views NOT in CLARITY_TBL. This catches custom
--            reporting views and anything else your org built on top of
--            Clarity. No English descriptions in this layer -- but at
--            least the table/column names round-trip into the schema.
--
-- ============================================================
-- HOW TO USE (typical SSMS workflow)
-- ============================================================
-- 1. Open this file in SSMS.
-- 2. Edit the @Tables list near the top.
-- 3. Connect to your Clarity database. Run (F5).
-- 4. Right-click results -> "Save Results As..." -> CSV.
--    Rename to clarity_metadata.csv (or whatever).
-- 5. If your views also reference objects in OTHER databases (e.g. a
--    separate Cook Clarity / Reporting DB), run this same query
--    connected to THAT database too -- with the relevant table list.
--    Save as a second CSV. Concatenate both CSVs (drop the duplicate
--    header row) before running csv_to_schema.py.
-- 6. Upload the CSV to Fabric. Convert via csv_to_schema.py to JSON.
-- 7. Point Tool 3's `schema_path` at the resulting JSON.
--
-- ============================================================
-- NOTES
-- ============================================================
-- 1. The CLARITY_TBL.TBL_DESCRIPTOR_OVR IS NOT NULL clause is intentional.
--    It dedupes rows where CLARITY_TBL has historical duplicates. Don't
--    remove unless you've confirmed dedup is handled another way.
-- 2. The sys.* fallback only sees the CURRENT database. If you have
--    objects in multiple databases, run the query once per database
--    (see step 5 above).
-- 3. csv_to_schema.py can accept rows where DESCRIPTION / TABLE_INTRO
--    are blank -- it will fall back to mechanical name expansion in
--    Tool 3's English output. So including bare sys.* rows is a net
--    improvement over the column not being in the schema at all (Tool 3
--    flags missing columns as `unknown_columns`).

-- ============================================================
-- EDIT THIS: which tables/views do you want metadata for?
-- ============================================================
-- Pre-populated for V_ACTIVE_MEMBERS. Once you've run Tool 1 on the
-- full view set, copy unique referenced_table values from the manifest
-- CSV into this list.

DECLARE @Tables TABLE (TABLE_NAME VARCHAR(100));
INSERT INTO @Tables (TABLE_NAME) VALUES
    -- V_ACTIVE_MEMBERS base tables (Layer A: Epic-documented):
    ('COVERAGE'),
    ('COVERAGE_MEMBER_LIST'),
    ('PATIENT'),
    ('PLAN_GRP_BEN_PLAN'),
    ('CLARITY_EPP'),
    ('CLARITY_LOB'),
    ('VALID_PATIENT'),
    ('CVG_LOC_PCP'),
    ('CLARITY_LOC'),
    ('CLARITY_SER'),
    ('ZC_SUBSC_RACE'),
    ('CVG_SUBSCR_ADDR'),
    ('ZC_TAX_STATE'),
    -- bi_complex referenced views (Layer B: system catalog fallback):
    ('V_CCHP_AuthHeader_Fact'),
    ('V_CCHP_UMAuthorizationRequest_Fact'),
    ('V_CCHP_UMAuthorizationRequestStatusHistory_Fact'),
    ('V_CCHP_UMAuthorization_Fact'),
    ('V_CCHP_UMAuthorizationHistory_Fact'),
    ('REFERRAL_HIST'),
    ('REFERRAL_BED_DAY'),
    ('REFERRAL'),
    ('REFERRAL_HISTORY'),
    ('RFL_HX_ACT'),
    ('RFL_HX_ITEM_CHANGE'),
    ('RFL_HX_NEW_VAL');
-- Add more rows as your view set grows. One per line.

-- ============================================================
-- LAYER A: Epic-documented Clarity tables
-- ============================================================
-- Output columns (must match csv_to_schema.py's expected headers):
--   TABLE_NAME           - table name (e.g. PATIENT)
--   TABLE_ID             - Clarity internal ID (NULL in Layer B)
--   TABLE_INTRODUCTION   - table description (NULL in Layer B)
--   COLUMN_NAME          - column name (e.g. PAT_ID)
--   DESCRIPTION          - column description (NULL in Layer B)
--   COLUMN_INI           - INI code (NULL in Layer B)
--   COLUMN_ITEM          - Item number (NULL in Layer B)

WITH clarity_documented AS (
    SELECT
        TBL.TABLE_NAME,
        CAST(TBL.TABLE_ID AS VARCHAR(50)) AS TABLE_ID,
        TBL.TABLE_INTRODUCTION,
        COL.COLUMN_NAME,
        COL.DESCRIPTION,
        INI.COLUMN_INI,
        INI.COLUMN_ITEM
    FROM CLARITY.dbo.CLARITY_TBL TBL
    JOIN CLARITY.dbo.CLARITY_COL COL
        ON TBL.TABLE_ID = COL.TABLE_ID
    LEFT JOIN CLARITY.dbo.CLARITY_COL_INIITM INI
        ON COL.COLUMN_ID = INI.COLUMN_ID
    WHERE TBL.TABLE_NAME IN (SELECT TABLE_NAME FROM @Tables)
      AND TBL.TBL_DESCRIPTOR_OVR IS NOT NULL
)
SELECT
    TABLE_NAME, TABLE_ID, TABLE_INTRODUCTION,
    COLUMN_NAME, DESCRIPTION, COLUMN_INI, COLUMN_ITEM
FROM clarity_documented

UNION ALL

-- ============================================================
-- LAYER B: System catalog fallback for objects not in CLARITY_TBL
-- ============================================================
-- Captures any tables/views the @Tables list mentions that Epic
-- doesn't document in CLARITY_TBL. Common cases: custom Cook Clarity /
-- Reporting views (V_CCHP_*), org-built reporting views, staging
-- tables.
SELECT
    o.name                AS TABLE_NAME,
    NULL                  AS TABLE_ID,
    NULL                  AS TABLE_INTRODUCTION,
    c.name                AS COLUMN_NAME,
    NULL                  AS DESCRIPTION,
    NULL                  AS COLUMN_INI,
    NULL                  AS COLUMN_ITEM
FROM sys.objects o
JOIN sys.columns c
    ON o.object_id = c.object_id
WHERE o.type IN ('U', 'V')      -- user table or view
  AND o.name IN (SELECT TABLE_NAME FROM @Tables)
  AND NOT EXISTS (
      SELECT 1
      FROM CLARITY.dbo.CLARITY_TBL TBL
      WHERE TBL.TABLE_NAME = o.name
        AND TBL.TBL_DESCRIPTOR_OVR IS NOT NULL
  )

ORDER BY TABLE_NAME, COLUMN_NAME;

-- ============================================================
-- TROUBLESHOOTING
-- ============================================================
-- "Invalid object name 'CLARITY.dbo.CLARITY_TBL'":
--    Your Clarity DB might not be named CLARITY. Try removing the
--    prefix: just CLARITY_TBL etc. Or check available DBs:
--    SELECT name FROM sys.databases;
--
-- "Invalid column name 'TBL_DESCRIPTOR_OVR'":
--    Older Clarity version. Drop that AND clause (you may get duplicate
--    rows but csv_to_schema.py will dedupe by TABLE_NAME + COLUMN_NAME).
--
-- "Invalid column name 'COLUMN_INI'":
--    Variable across Clarity versions. Run:
--    SELECT TOP 1 * FROM CLARITY.dbo.CLARITY_COL_INIITM;
--    to see the actual columns. Common alternatives: INI_NAME, ITEM_NUM.
--
-- Result set is empty:
--    Either @Tables list mismatches Clarity's TABLE_NAME casing, OR
--    your account lacks SELECT on CLARITY_TBL/COL/INIITM/sys.objects.
--    Try a simpler diagnostic:
--    SELECT TOP 5 TABLE_NAME FROM CLARITY.dbo.CLARITY_TBL;
--    SELECT TOP 5 name FROM sys.objects WHERE type IN ('U','V');
--
-- Some views/tables come back in Layer B with NULL descriptions:
--    Expected -- those aren't documented in CLARITY_TBL. csv_to_schema.py
--    accepts the NULLs; Tool 3 falls back to mechanical abbreviation
--    expansion for those, and flags them in `unknown_columns` so you
--    can see the data-dictionary backlog.
