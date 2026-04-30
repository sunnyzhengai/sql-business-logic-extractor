-- Reproduces the exact SELECT-list shape from the user's real production view
-- (first 10-ish columns) so we can see which references make it into the
-- manifest. Aliases: CVGEPT, CVG, LOB, EPP, SER. The FROM/JOIN clause is
-- synthesized to make all aliases resolve; only the SELECT shape is what we
-- care about.
CREATE VIEW dbo.t99_real_view_first_10 AS
SELECT [ID] = CVGEPT.PAT_ID
    ,[Coverage ID]                       = CVG.COVERAGE_ID
    ,[Name]                              = CVG.SUBSCR_NAME
    ,[Member ID]                         = CVGEPT.MEM_NUMBER
    ,[Coverage Effective Date - 400]     = convert(date, CVG.CVG_EFF_DT)
    ,[Coverage Term Date 410]            = convert(date, CVG.CVG_TERM_DT)
    ,[Member Effective From Date 320]    = convert(date, CVGEPT.MEM_EFF_FROM_DATE)
    ,[Member Effective Term Date 330]    = convert(date, CVGEPT.MEM_EFF_TO_DATE)
    ,[LOB]                               = LOB.LOB_NAME
    ,[Plan Name]                         = EPP.BENEFIT_PLAN_NAME
    ,[PCP]                               = SER.PROV_NAME
FROM Clarity.dbo.COVERAGE CVG
INNER JOIN Clarity.dbo.COVERAGE_PATIENT     CVGEPT ON CVGEPT.COVERAGE_ID = CVG.COVERAGE_ID
INNER JOIN Clarity.dbo.LINE_OF_BUSINESS     LOB    ON LOB.LOB_ID         = CVG.LOB_ID
INNER JOIN Clarity.dbo.EMPLOYER_PLAN        EPP    ON EPP.PLAN_ID        = CVG.PLAN_ID
LEFT JOIN Clarity.dbo.SERVICE_PROVIDER      SER    ON SER.PROV_ID        = CVGEPT.PCP_PROV_ID;
