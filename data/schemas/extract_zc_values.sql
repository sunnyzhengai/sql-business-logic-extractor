-- Epic Clarity ZC code-to-name extract for tools/extract_corpus.
--
-- Walks every ZC_* table registered in CLARITY_TBL and emits a flat
-- (zc_table, code, name) result set. The CSV produced from this
-- query becomes the data dictionary for resolving filter predicates
-- like `COVERAGE_TYPE_C = 2` to their human-readable names
-- ("Managed Care") during corpus extraction.
--
-- Output columns:
--     zc_table  -- e.g., ZC_COVERAGE_TYPE
--     code      -- the numeric code as a string (preserves leading zeros)
--     name      -- the NAME column value
--
-- ============================================================
-- HOW TO USE (typical SSMS workflow)
-- ============================================================
-- 1. Open this file in SSMS. Connect to a database where the
--    Clarity metadata tables (CLARITY_TBL, CLARITY_COL) and the
--    actual ZC_* tables are reachable as `CLARITY.dbo.<name>`.
--    On most Epic deployments this is the Clarity reporting DB.
-- 2. Run (F5). The Messages tab will print the length and first
--    chunk of the generated dynamic SQL (debug aid). The result
--    grid will show one row per (ZC_table, code, name).
-- 3. Right-click results -> "Save Results As..." -> CSV.
--    Save as zc_values.csv. Verify the header row reads:
--       zc_table,code,name
--    SSMS sometimes saves without a header -- if missing, add it
--    manually as the first line.
-- 4. Upload the CSV to Fabric at:
--       data/dictionaries/zc_values.csv
--    OR pass an explicit path via:
--       extract_corpus(..., zc_values_path='/path/to/zc_values.csv')
-- 5. Re-run extract_corpus. Filter predicates of the form `<X>_C = <N>`
--    will now resolve via this dictionary, surfacing as
--    `filter.zc_lookups` in corpus.jsonl and as `/* name */` inline
--    annotations in cohort_extract output.
--
-- ============================================================
-- HOW IT WORKS
-- ============================================================
-- Epic Clarity's ZC_* tables follow a uniform naming convention:
--   - The table is `ZC_<X>`
--   - The code column is `<X>_C`            (e.g., COVERAGE_TYPE_C)
--   - The label column is always named NAME
-- We use CLARITY_TBL to find every table whose TABLE_NAME starts with
-- `ZC_`, derive the code-column name by stripping the `ZC_` prefix
-- and appending `_C`, and verify both the code column and a NAME
-- column exist via CLARITY_COL. Then dynamic SQL unions one
-- `SELECT zc_table, code, NAME FROM CLARITY.dbo.<table>` per match.
--
-- ============================================================
-- NOTES
-- ============================================================
-- 1. The `TBL.TBL_DESCRIPTOR_OVR IS NOT NULL` clause matches the
--    convention from extract_clarity_metadata.sql -- it dedupes
--    historical-duplicate CLARITY_TBL rows. If your install doesn't
--    have this issue, removing the clause is harmless.
-- 2. `TBL.TABLE_NAME LIKE 'ZC[_]%'` -- the bracket escapes the `_`
--    so we don't match tables like `ZCOMPANY` that happen to start
--    with `ZC` plus any character.
-- 3. The CAST to NVARCHAR(50) for `code` keeps numeric codes as
--    strings -- preserves leading zeros and avoids locale issues
--    with numeric formatting in CSV export.
-- 4. If your Clarity universe spans multiple databases, run this
--    query connected to each and concatenate the CSVs (drop the
--    duplicate header rows).

SET NOCOUNT ON;
DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql = @sql +
    CASE WHEN @sql = N'' THEN N'' ELSE N' UNION ALL ' END +
    N'SELECT ''' + TBL.TABLE_NAME + N''' AS zc_table, ' +
    N'CAST(' + QUOTENAME(SUBSTRING(TBL.TABLE_NAME, 4, 256) + '_C')
        + N' AS NVARCHAR(50)) AS code, ' +
    N'CAST([NAME] AS NVARCHAR(500)) AS [name] ' +
    N'FROM CLARITY.dbo.' + QUOTENAME(TBL.TABLE_NAME)
FROM CLARITY.dbo.CLARITY_TBL TBL
WHERE TBL.TABLE_NAME LIKE 'ZC[_]%'
  AND TBL.TBL_DESCRIPTOR_OVR IS NOT NULL
  AND EXISTS (
        SELECT 1
        FROM CLARITY.dbo.CLARITY_COL COL
        WHERE COL.TABLE_ID = TBL.TABLE_ID
          AND COL.COLUMN_NAME = SUBSTRING(TBL.TABLE_NAME, 4, 256) + '_C'
      )
  AND EXISTS (
        SELECT 1
        FROM CLARITY.dbo.CLARITY_COL COL
        WHERE COL.TABLE_ID = TBL.TABLE_ID
          AND COL.COLUMN_NAME = 'NAME'
      );

-- DEBUG: confirm the dynamic SQL got built. Check the Messages tab.
PRINT 'Length of generated SQL: ' + CAST(LEN(@sql) AS NVARCHAR(20));
PRINT LEFT(@sql, 2000);

EXEC sp_executesql @sql;
