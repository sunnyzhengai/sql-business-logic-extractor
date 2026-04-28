-- Test: older T-SQL "[alias] = expr" syntax (in addition to "expr AS [alias]"),
-- including bracketed identifiers with spaces and special characters in the
-- alias.
CREATE VIEW dbo.t04_alias_equals_syntax AS
SELECT
    [Coverage Effective Date - 400] = CONVERT(date, CVG.CVG_EFF_DT),
    [Patient ID]                    = CVG.PAT_ID,
    Coverage_Type                   = CVG.CVG_TYPE_C
FROM Clarity.dbo.COVERAGE CVG;
