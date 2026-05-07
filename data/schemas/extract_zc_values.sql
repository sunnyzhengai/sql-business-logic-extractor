-- Epic Clarity ZC code-to-name extract for tools/extract_corpus.
--
-- Walks every ZC_* table in the connected Clarity database and emits
-- a flat (zc_table, code, name) result set. The CSV produced from
-- this query becomes the data dictionary for resolving filter
-- predicates like `COVERAGE_TYPE_C = 2` to their human-readable
-- names ("Managed Care") during corpus extraction.
--
-- Output columns:
--     zc_table  -- e.g., ZC_COVERAGE_TYPE
--     code      -- the numeric code as a string (preserves leading zeros)
--     name      -- the NAME column value
--
-- ============================================================
-- HOW TO USE (typical SSMS workflow)
-- ============================================================
-- 1. Open this file in SSMS connected to your Clarity database.
-- 2. Run (F5). The result grid will show one row per (ZC_table, code, name).
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
-- Epic Clarity's ZC_* tables follow a uniform shape:
--   - One column ending in `_C` (the numeric code, FK target)
--   - One column named `NAME` (the human label)
--   - Other columns (description, abbreviations, etc.) we ignore.
--
-- The query auto-discovers every ZC_* table, picks the first `_C`
-- column as the code, and unions the (table, code, NAME) tuples into
-- a single result set via dynamic SQL. No hand-maintenance of a
-- per-table list required.
--
-- ============================================================
-- NOTES
-- ============================================================
-- 1. `t.name LIKE 'ZC[_]%'` is intentional -- the bracket escapes the
--    `_` so we don't match tables that happen to start with `ZC` plus
--    any character (e.g., `ZCOMPANY`).
-- 2. `c.column_id = (SELECT MIN(column_id) ...)` picks the first `_C`
--    column in physical declaration order. ~99% of Clarity ZC tables
--    have exactly one `_C` column. For the rare exceptions, this
--    picks the first; fix-forward by tightening the join if you find
--    a specific ZC table that resolves wrong.
-- 3. The CAST to NVARCHAR(50) for `code` lets us keep numeric codes
--    as strings in the CSV -- preserves leading zeros for codes that
--    have them, and avoids locale issues with numeric formatting.
-- 4. If your Clarity universe spans multiple databases (e.g., a
--    separate reporting DB built on top of Clarity), run this query
--    once per database, save each result, and concatenate the CSVs
--    (drop the duplicate header rows).

SET NOCOUNT ON;
DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql = @sql +
    CASE WHEN @sql = N'' THEN N'' ELSE N' UNION ALL ' END +
    N'SELECT ''' + t.name + N''' AS zc_table, ' +
    N'CAST(' + QUOTENAME(c.name) + N' AS NVARCHAR(50)) AS code, ' +
    N'CAST([NAME] AS NVARCHAR(500)) AS [name] ' +
    N'FROM ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name)
FROM sys.tables t
JOIN sys.columns c ON c.object_id = t.object_id
WHERE t.name LIKE 'ZC[_]%'
  AND c.name LIKE '%[_]C'
  AND c.column_id = (
        SELECT MIN(column_id)
        FROM sys.columns
        WHERE object_id = t.object_id AND name LIKE '%[_]C'
      )
  AND EXISTS (
        SELECT 1 FROM sys.columns nm
        WHERE nm.object_id = t.object_id AND nm.name = 'NAME'
      );

EXEC sp_executesql @sql;
