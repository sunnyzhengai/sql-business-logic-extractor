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
-- 3. (OPTIONAL but recommended) Edit the @Columns list to narrow to
--    just the columns your views actually use. Source the (table,
--    column) pairs from Tool 1's column_lineage_extractor.csv manifest
--    (referenced_table + referenced_column columns). Skipping this
--    step pulls EVERY column for each table -- works, just bigger.
-- 4. Connect to your Clarity database. Run (F5).
-- 5. Right-click results -> "Save Results As..." -> CSV.
--    Rename to clarity_metadata.csv (or whatever).
-- 6. If your views also reference objects in OTHER databases (e.g. a
--    separate Cook Clarity / Reporting DB), run this same query
--    connected to THAT database too -- with the relevant table list.
--    Save as a second CSV. Concatenate both CSVs (drop the duplicate
--    header row) before running csv_to_schema.py.
-- 7. Upload the CSV to Fabric. Convert via csv_to_schema.py to JSON.
-- 8. Point Tool 3's `schema_path` at the resulting JSON.
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
-- OPTIONAL: narrow to ONLY the columns your views actually use
-- ============================================================
-- By default this script returns EVERY column on each table in @Tables.
-- That's fine for small tables, but Clarity tables like PATIENT have
-- 1000+ columns -- most of which your views don't touch. To keep the
-- CSV (and Tool 3's schema) lean, populate @Columns below with the
-- (TABLE_NAME, COLUMN_NAME) pairs from Tool 1's manifest.
--
-- Source the pairs from the column_lineage_extractor.csv manifest:
--   SELECT DISTINCT referenced_table, referenced_column
--   FROM column_lineage_extractor.csv
--   WHERE referenced_table IS NOT NULL
--     AND referenced_column IS NOT NULL;
-- Then paste the pairs into the VALUES list below.
--
-- Leave @Columns empty (don't INSERT any rows) to get ALL columns --
-- the @UseColumnFilter switch below auto-detects an empty list.

DECLARE @Columns TABLE (TABLE_NAME VARCHAR(100), COLUMN_NAME VARCHAR(100));
INSERT INTO @Columns (TABLE_NAME, COLUMN_NAME) VALUES
    -- ----- V_ACTIVE_MEMBERS referenced columns -----
    ('CLARITY_EPP', 'BENEFIT_PLAN_ID'),
    ('CLARITY_EPP', 'BENEFIT_PLAN_NAME'),
    ('CLARITY_LOB', 'LOB_ID'),
    ('CLARITY_LOB', 'LOB_NAME'),
    ('CLARITY_LOC', 'LOC_ID'),
    ('CLARITY_LOC', 'LOC_NAME'),
    ('CLARITY_SER', 'PROV_ID'),
    ('CLARITY_SER', 'PROV_NAME'),
    ('COVERAGE', 'COVERAGE_ID'),
    ('COVERAGE', 'COVERAGE_TYPE_C'),
    ('COVERAGE', 'CVG_EFF_DT'),
    ('COVERAGE', 'CVG_TERM_DT'),
    ('COVERAGE', 'PLAN_GRP_ID'),
    ('COVERAGE', 'SUBSCR_BIRTHDATE'),
    ('COVERAGE', 'SUBSCR_NAME'),
    ('COVERAGE', 'SUBSCR_STATE_C'),
    ('COVERAGE', 'SUBSCR_ZIP'),
    ('COVERAGE', 'SUBSC_RACE_C'),
    ('COVERAGE_MEMBER_LIST', 'COVERAGE_ID'),
    ('COVERAGE_MEMBER_LIST', 'LINE'),
    ('COVERAGE_MEMBER_LIST', 'MEM_COVERED_YN'),
    ('COVERAGE_MEMBER_LIST', 'MEM_EFF_FROM_DATE'),
    ('COVERAGE_MEMBER_LIST', 'MEM_EFF_TO_DATE'),
    ('COVERAGE_MEMBER_LIST', 'MEM_NUMBER'),
    ('COVERAGE_MEMBER_LIST', 'PAT_ID'),
    ('CVG_LOC_PCP', 'COVERAGE_ID'),
    ('CVG_LOC_PCP', 'DELETED_FLAG_YN'),
    ('CVG_LOC_PCP', 'EFF_DATE'),
    ('CVG_LOC_PCP', 'LOCATION_ID'),
    ('CVG_LOC_PCP', 'MEMBER_ID'),
    ('CVG_LOC_PCP', 'PCP_ID'),
    ('CVG_LOC_PCP', 'TERM_DATE'),
    ('CVG_SUBSCR_ADDR', 'CVG_ID'),
    ('CVG_SUBSCR_ADDR', 'LINE'),
    ('CVG_SUBSCR_ADDR', 'SUBSCR_ADDR'),
    ('PATIENT', 'PAT_ID'),
    ('PLAN_GRP_BEN_PLAN', 'BEN_PLAN_EFF_DATE'),
    ('PLAN_GRP_BEN_PLAN', 'BEN_PLAN_ID'),
    ('PLAN_GRP_BEN_PLAN', 'BEN_PLAN_TERM_DT'),
    ('PLAN_GRP_BEN_PLAN', 'PLAN_GRP_ID'),
    ('PLAN_GRP_BEN_PLAN', 'PLAN_LOB_ID'),
    ('VALID_PATIENT', 'IS_VALID_PAT_YN'),
    ('VALID_PATIENT', 'PAT_ID'),
    ('ZC_SUBSC_RACE', 'NAME'),
    ('ZC_SUBSC_RACE', 'SUBSC_RACE_C'),
    ('ZC_TAX_STATE', 'NAME'),
    ('ZC_TAX_STATE', 'TAX_STATE_C'),
    -- ----- bi_complex referenced columns -----
    ('REFERRAL', 'REFERRAL_ID'),
    ('REFERRAL', 'REFERRAL_PROV_ID'),
    ('REFERRAL', 'REFERRING_PROV_ID'),
    ('REFERRAL', 'START_DATE'),
    ('REFERRAL_BED_DAY', 'BED_DAY_STATUS_C'),
    ('REFERRAL_BED_DAY', 'REFERRAL_ID'),
    ('REFERRAL_HIST', 'HX_USER_ID'),
    ('REFERRAL_HIST', 'LINE'),
    ('REFERRAL_HIST', 'NEW_RFL_STATUS_C'),
    ('REFERRAL_HIST', 'REFERRAL_ID'),
    ('RFL_HX_ACT', 'ACTION_DTTM'),
    ('RFL_HX_ACT', 'GROUP_LINE'),
    ('RFL_HX_ACT', 'REFERRAL_ID'),
    ('RFL_HX_ACT', 'VALUE_LINE'),
    ('RFL_HX_ITEM_CHANGE', 'GROUP_LINE'),
    ('RFL_HX_ITEM_CHANGE', 'ITEM_CHANGE'),
    ('RFL_HX_ITEM_CHANGE', 'REFERRAL_ID'),
    ('RFL_HX_ITEM_CHANGE', 'VALUE_LINE'),
    ('RFL_HX_NEW_VAL', 'GROUP_LINE'),
    ('RFL_HX_NEW_VAL', 'NEW_VAL'),
    ('RFL_HX_NEW_VAL', 'NEW_VALUE_EXTERNAL'),
    ('RFL_HX_NEW_VAL', 'REFERRAL_ID'),
    ('RFL_HX_NEW_VAL', 'VALUE_LINE'),
    -- ----- bi_complex V_CCHP_* fact-view columns (Layer B fallback) -----
    -- These camelCase columns live on the V_CCHP_AuthHeader_Fact view; Tool 1
    -- attributed some to REFERRAL because the alias RFL is shared. Listing them
    -- under both is harmless -- only matching (table, column) pairs get rows.
    ('V_CCHP_AuthHeader_Fact', 'BedDayAuthRequestId'),
    ('V_CCHP_AuthHeader_Fact', 'Coverageid'),
    ('V_CCHP_AuthHeader_Fact', 'CustomEntryDateTime'),
    ('V_CCHP_AuthHeader_Fact', 'EntryDate'),
    ('V_CCHP_AuthHeader_Fact', 'Externalid'),
    ('V_CCHP_AuthHeader_Fact', 'Referralid'),
    ('V_CCHP_AuthHeader_Fact', 'ReferralLineOfBusinessId'),
    ('V_CCHP_AuthHeader_Fact', 'ReferralPriority'),
    ('V_CCHP_AuthHeader_Fact', 'ReferralProviderId'),
    ('V_CCHP_AuthHeader_Fact', 'ReferralTypeCategory'),
    ('V_CCHP_AuthHeader_Fact', 'ReferringProviderId'),
    ('V_CCHP_AuthHeader_Fact', 'StartDate'),
    -- Tool 1 mis-attributed copies under REFERRAL; keep too in case columns
    -- really do exist on REFERRAL with these names:
    ('REFERRAL', 'BedDayAuthRequestId'),
    ('REFERRAL', 'Coverageid'),
    ('REFERRAL', 'CustomEntryDateTime'),
    ('REFERRAL', 'EntryDate'),
    ('REFERRAL', 'Externalid'),
    ('REFERRAL', 'Referralid'),
    ('REFERRAL', 'ReferralLineOfBusinessId'),
    ('REFERRAL', 'ReferralPriority'),
    ('REFERRAL', 'ReferralProviderId'),
    ('REFERRAL', 'ReferralTypeCategory'),
    ('REFERRAL', 'ReferringProviderId'),
    ('REFERRAL', 'StartDate'),
    ('V_CCHP_UMAuthorizationHistory_Fact', 'AUTH_ID'),
    ('V_CCHP_UMAuthorizationHistory_Fact', 'CHANGED_BY_USER_ID'),
    ('V_CCHP_UMAuthorizationHistory_Fact', 'CHANGED_DTTM'),
    ('V_CCHP_UMAuthorizationHistory_Fact', 'UM_DENIED_RSN'),
    ('V_CCHP_UMAuthorizationHistory_Fact', 'UM_PENDING_RSN'),
    ('V_CCHP_UMAuthorizationHistory_Fact', 'UM_STATUS'),
    ('V_CCHP_UMAuthorizationRequestStatusHistory_Fact', 'AUTH_REQUEST_ID'),
    ('V_CCHP_UMAuthorizationRequestStatusHistory_Fact', 'UM_STATUS_C'),
    ('V_CCHP_UMAuthorization_Fact', 'AUTH_ID'),
    ('V_CCHP_UMAuthorization_Fact', 'UM_AUTH_REQUEST_ID');

DECLARE @UseColumnFilter BIT =
    CASE WHEN EXISTS (SELECT 1 FROM @Columns WHERE TABLE_NAME <> '') THEN 1 ELSE 0 END;

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
      AND (
          @UseColumnFilter = 0
          OR EXISTS (
              SELECT 1 FROM @Columns C
              WHERE C.TABLE_NAME = TBL.TABLE_NAME
                AND C.COLUMN_NAME = COL.COLUMN_NAME
          )
      )
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
  AND (
      @UseColumnFilter = 0
      OR EXISTS (
          SELECT 1 FROM @Columns C
          WHERE C.TABLE_NAME = o.name
            AND C.COLUMN_NAME = c.name
      )
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
