-- Cleaned from OCR'd source: V_ACTIVE_MEMBERS.sql
-- See sibling file for the raw OCR output and the cleanup notes in the
-- conversation log. Inferred structural fix: the top SELECT was wrapped
-- in a CTE called `ActiveMembers` (the closing `)` on the original line
-- 77 plus the trailing `SELECT * FROM ActiveMembers WHERE [ROW] = 1`
-- imply the original had `WITH ActiveMembers AS (...)` -- OCR dropped
-- that opening line.

WITH ActiveMembers AS (
    SELECT
        [ID]                              = CVGEPT.PAT_ID
        , [Coverage ID]                   = CVG.COVERAGE_ID
        , [Name]                          = CVG.SUBSCR_NAME
        , [Member ID]                     = CVGEPT.MEM_NUMBER
        , [Coverage Effective Date - 400] = convert(date, CVG.CVG_EFF_DT)
        , [Coverage Term Date 410]        = convert(date, CVG.CVG_TERM_DT)
        , [Member Effective From Date 320] = convert(date, CVGEPT.MEM_EFF_FROM_DATE)
        , [Member Effective Term Date 330] = convert(date, CVGEPT.MEM_EFF_TO_DATE)
        , [LOB]                           = LOB.LOB_NAME
        , [Plan Name]                     = EPP.BENEFIT_PLAN_NAME
        , [PCP]                           = SER.PROV_NAME
        , [Location Name]                 = LOC.LOC_NAME
        , [Birth Date]                    = CVG.SUBSCR_BIRTHDATE
        , [ROW]                           = ROW_NUMBER() OVER (
                                                PARTITION BY CVGEPT.PAT_ID
                                                ORDER BY CVGEPT.MEM_EFF_FROM_DATE DESC,
                                                         ISNULL(CVGEPT.MEM_EFF_TO_DATE, '29991231') DESC
                                              )
        , [Race]                          = ZSR.NAME
        , [Member Address 1]              = CSA.SUBSCR_ADDR
        , [Member Address 2]              = CSA2.SUBSCR_ADDR
        , [Member City]                   = SUBSCR_CITY
        , [Member State]                  = ZTS.NAME
        , [Member Zip Code]               = CVG.SUBSCR_ZIP
        , [Coverage Line]                 = CVGEPT.LINE
    FROM Clarity.dbo.COVERAGE CVG
        JOIN Clarity.dbo.COVERAGE_MEMBER_LIST CVGEPT
            ON CVGEPT.COVERAGE_ID = CVG.COVERAGE_ID
           AND CVG.COVERAGE_TYPE_C = 2  -- Managed Care
           -- AND CVG.CVG_EFF_DT <= SYSDATETIME()
           -- AND (CVG.CVG_TERM_DT >= SYSDATETIME() OR CVG.CVG_TERM_DT IS NULL)
           AND CVGEPT.MEM_COVERED_YN = 'Y'
           AND CVGEPT.MEM_EFF_FROM_DATE <= SYSDATETIME()
           AND (CVGEPT.MEM_EFF_TO_DATE >= SYSDATETIME() OR CVGEPT.MEM_EFF_TO_DATE IS NULL)
        JOIN Clarity.dbo.PLAN_GRP_BEN_PLAN PGEPP
            ON PGEPP.PLAN_GRP_ID = CVG.PLAN_GRP_ID
           AND PGEPP.BEN_PLAN_EFF_DATE <= SYSDATETIME()
           AND (PGEPP.BEN_PLAN_TERM_DT >= SYSDATETIME() OR PGEPP.BEN_PLAN_TERM_DT IS NULL)
        JOIN Clarity.dbo.CLARITY_EPP EPP
            ON EPP.BENEFIT_PLAN_ID = PGEPP.BEN_PLAN_ID
        JOIN Clarity.dbo.CLARITY_LOB LOB
            ON LOB.LOB_ID = PGEPP.PLAN_LOB_ID
        JOIN Clarity.dbo.PATIENT EPT
            ON EPT.PAT_ID = CVGEPT.PAT_ID
        JOIN Clarity.dbo.VALID_PATIENT EPTV
            ON EPTV.PAT_ID = CVGEPT.PAT_ID
           AND EPTV.IS_VALID_PAT_YN = 'Y'
        LEFT JOIN Clarity.dbo.CVG_LOC_PCP CVGLOCSER
            ON CVGLOCSER.COVERAGE_ID = CVG.COVERAGE_ID
           AND CVGLOCSER.MEMBER_ID = CVGEPT.PAT_ID
           AND CVGLOCSER.EFF_DATE <= SYSDATETIME()
           AND (CVGLOCSER.TERM_DATE >= SYSDATETIME() OR CVGLOCSER.TERM_DATE IS NULL)
           AND (CVGLOCSER.DELETED_FLAG_YN = 'N' OR CVGLOCSER.DELETED_FLAG_YN IS NULL)
        LEFT JOIN Clarity.dbo.CLARITY_LOC LOC
            ON LOC.LOC_ID = CVGLOCSER.LOCATION_ID
        LEFT JOIN Clarity.dbo.CLARITY_SER SER
            ON SER.PROV_ID = CVGLOCSER.PCP_ID
        LEFT JOIN Clarity.dbo.ZC_SUBSC_RACE ZSR
            ON ZSR.SUBSC_RACE_C = CVG.SUBSC_RACE_C
        LEFT JOIN Clarity.dbo.CVG_SUBSCR_ADDR CSA
            ON CSA.CVG_ID = CVG.COVERAGE_ID
           AND CSA.LINE = 1
        LEFT JOIN Clarity.dbo.CVG_SUBSCR_ADDR CSA2
            ON CSA2.CVG_ID = CVG.COVERAGE_ID
           AND CSA2.LINE = 2
        LEFT JOIN Clarity.dbo.ZC_TAX_STATE ZTS
            ON ZTS.TAX_STATE_C = CVG.SUBSCR_STATE_C
)
SELECT *
FROM ActiveMembers
WHERE [ROW] = 1;
