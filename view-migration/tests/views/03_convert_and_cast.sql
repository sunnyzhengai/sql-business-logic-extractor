-- Test: T-SQL CONVERT and CAST functions must NOT swallow the column reference
-- inside them. The column should still appear in the manifest with its
-- table qualifier resolved.
CREATE VIEW dbo.t03_convert_and_cast AS
SELECT
    CVG.PAT_ID,
    CONVERT(date, CVG.CVG_EFF_DT) AS Coverage_Effective_Date,
    CONVERT(varchar(50), CVG.CVG_TYPE_C, 101) AS CVG_Type_Text,
    CAST(CVG.PAT_ID AS varchar(20)) AS PAT_ID_TXT
FROM Clarity.dbo.COVERAGE CVG;
