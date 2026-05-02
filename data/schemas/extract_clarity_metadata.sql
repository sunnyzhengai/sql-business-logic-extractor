-- Epic Clarity metadata extract for Tool 3 (business_logic_extractor).
--
-- Pulls table + column descriptions + INI/Item codes for the tables
-- your views reference. Output is a flat result set you save to CSV;
-- the CSV is then converted to a schema YAML/JSON via the sibling
-- script data/schemas/csv_to_schema.py.
--
-- ============================================================
-- HOW TO USE (typical SSMS workflow)
-- ============================================================
-- 1. Open this file in SSMS.
-- 2. Edit the @TABLES list near the top (line ~30).
-- 3. Connect to your Clarity database.
-- 4. Run the query (F5).
-- 5. In the results pane, right-click any cell -> "Save Results As..."
-- 6. Save as CSV. SSMS default name is `Results.csv`; rename to
--    `clarity_metadata.csv` for clarity.
-- 7. Upload that CSV to Fabric (drag-and-drop into the Lakehouse Files
--    area, or wherever your data flows).
-- 8. On Fabric, run csv_to_schema.py to convert CSV -> JSON. Or do it
--    locally and upload the JSON.
--
-- ============================================================
-- NOTES
-- ============================================================
-- 1. The TBL_DESCRIPTOR_OVR IS NOT NULL clause is intentional. It dedupes
--    CLARITY_TBL rows that appear twice (some Clarity installs have
--    historical duplicates). Don't remove without confirming the dedup
--    is handled another way.
-- 2. CLARITY_COL_INIITM holds the (INI, Item) coordination key per
--    column. This is the join Epic uses to match a column to its
--    Chronicles source -- valuable for Collibra ingestion later.
-- 3. Cook Clarity / Reporting views (V_CCHP_*) are NOT in CLARITY_TBL
--    -- they're Cook-curated views. Their metadata lives in different
--    tables (or might not exist at all). This query only documents
--    base Clarity tables.

-- ============================================================
-- EDIT THIS: which tables do you want metadata for?
-- ============================================================
-- For your first run on V_ACTIVE_MEMBERS, this list is pre-populated.
-- For the full set, run Tool 1 (column_lineage_extractor) first, take
-- the unique referenced_table values from manifest.csv, paste here.

DECLARE @Tables TABLE (TABLE_NAME VARCHAR(50));
INSERT INTO @Tables (TABLE_NAME) VALUES
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
    ('ZC_TAX_STATE');
-- Add more rows as your view set grows. One per line.

-- ============================================================
-- THE QUERY
-- ============================================================
-- Output columns (these are exactly what csv_to_schema.py expects):
--   TABLE_NAME           - Clarity table name (e.g. PATIENT)
--   TABLE_ID             - Clarity internal table ID
--   TABLE_INTRODUCTION   - Table-level description text
--   COLUMN_NAME          - Column name (e.g. PAT_ID)
--   DESCRIPTION          - Column-level description
--   COLUMN_INI           - INI code (e.g. EAF for PATIENT)
--   COLUMN_ITEM          - Item number (Chronicles coordination key)

SELECT
    TBL.TABLE_NAME,
    TBL.TABLE_ID,
    TBL.TABLE_INTRODUCTION,
    COL.COLUMN_NAME,
    COL.DESCRIPTION,
    INI.COLUMN_INI,
    INI.COLUMN_ITEM
FROM CLARITY.dbo.CLARITY_TBL TBL
JOIN CLARITY.dbo.CLARITY_COL COL
    ON TBL.TABLE_ID = COL.TABLE_ID
LEFT JOIN CLARITY.dbo.CLARITY_COL_INIITM INI    -- LEFT so columns without INI/Item still appear
    ON COL.COLUMN_ID = INI.COLUMN_ID
WHERE TBL.TABLE_NAME IN (SELECT TABLE_NAME FROM @Tables)
  AND TBL.TBL_DESCRIPTOR_OVR IS NOT NULL        -- dedup CLARITY_TBL duplicates
ORDER BY TBL.TABLE_NAME, COL.COLUMN_ID;

-- ============================================================
-- TROUBLESHOOTING
-- ============================================================
-- "Invalid object name 'CLARITY.dbo.CLARITY_TBL'":
--    Your Clarity DB might not be named CLARITY or the schema might
--    not be dbo. Try removing the prefix: just CLARITY_TBL etc.
--    Or check with: SELECT name FROM sys.databases;
--
-- "Invalid column name 'TBL_DESCRIPTOR_OVR'":
--    Clarity version variation. Drop that AND clause -- you may get
--    duplicate rows but csv_to_schema.py will dedupe.
--
-- "Invalid column name 'COLUMN_INI'":
--    Some installs use different names (e.g. INI_NAME, ITEM_NUM).
--    Run: SELECT TOP 1 * FROM CLARITY.dbo.CLARITY_COL_INIITM;
--    to see the actual column names, then edit the SELECT list.
--
-- Result set is empty:
--    Either your @Tables list doesn't match Clarity's TABLE_NAME
--    casing/spelling, OR your account doesn't have SELECT on
--    CLARITY_TBL/COL/COL_INIITM. Check with the simpler diagnostic:
--    SELECT TOP 5 TABLE_NAME FROM CLARITY.dbo.CLARITY_TBL;
