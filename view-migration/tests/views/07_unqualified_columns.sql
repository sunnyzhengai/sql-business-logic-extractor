-- Test: unqualified column references (no alias prefix). Confidence should
-- drop to "low" or "medium" depending on context, since the parser can't
-- always pin a bare column to a specific table when there are multiple in
-- scope. Single-table queries should still resolve the column cleanly.
CREATE VIEW dbo.t07_unqualified_columns AS
SELECT
    PAT_ID,
    PAT_NAME,
    BIRTH_DATE
FROM Clarity.dbo.PATIENT;
