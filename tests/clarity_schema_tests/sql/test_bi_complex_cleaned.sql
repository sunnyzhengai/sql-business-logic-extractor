-- OCR-cleaned version of test_bi_complex.sql.
-- Changes are strictly typographic: dot/colon/dash fixes, alias corrections,
-- stray-space removal, and added WITH + minimal main SELECT so it parses.
-- No column names, table names, filters, or logic were changed.

WITH ReferralDenials AS (

    SELECT

        [REFERRAL_ID]        = RFL.Referralid,

        RFL.BedDayAuthRequestId,

        [REFERRAL_EXT_ID]    = RFL.Externalid,                                   --Sla

        [COVERAGE_ID]        = RFL.Coverageid,

        [ENTRY_DATE]         = RFL.EntryDate,

        [START_DATE]         = RFL.StartDate,

        [PRIORITY]           = RFL.ReferralPriority,

        [RFL_LOB_ID]         = RFL.ReferralLineOfBusinessId,

        [REFERRAL_PROV_ID]   = RFL.ReferralProviderId,

        [REFERRING_PROV_ID]  = RFL.ReferringProviderId,

        [RFL_TYPE_C]         = RFL.ReferralTypeCategory,

        RFL.CustomEntryDateTime                                                  --Sla

    FROM

        CookClarity.Reporting.V_CCHP_AuthHeader_Fact RFL

    WHERE

        0 = 0

        AND EXISTS (

            SELECT 1
            FROM   Clarity.dbo.REFERRAL_HIST RFLH
            WHERE  RFLH.REFERRAL_ID        = RFL.Referralid
              AND  RFLH.NEW_RFL_STATUS_C   = 5                                   -- Denied

            UNION ALL

            SELECT 1
            FROM   Clarity.dbo.REFERRAL_BED_DAY RFLBD
            WHERE  RFL.Referralid          = RFLBD.REFERRAL_ID
              AND  RFLBD.BED_DAY_STATUS_C  = 2                                   -- Denied
        )

)

, AugDenialSla AS (

    SELECT

        aug.AUTH_REQUEST_ID,

        aug.AUTH_REQUEST_EXTERNAL_ID,

        aug.REFERRAL_ID,
        aug.COVERAGE_ID,

        aug.AUG_RECEIVED_DTTM,

        COALESCE(aug.START_DATE, rfl.[START_DATE]) AS [START_DATE],              --may

        aug.[PRIORITY],

        aug.LOB_ID,

        rfl.REFERRAL_PROV_ID,

        rfl.REFERRING_PROV_ID,

        aug.RFL_TYPE_C

    FROM

        CookClarity.Reporting.V_CCHP_UMAuthorizationRequest_Fact aug

        JOIN Clarity.dbo.REFERRAL rfl
            ON rfl.REFERRAL_ID = aug.REFERRAL_ID

    WHERE

        0 = 0

        AND EXISTS (

            SELECT 1
            FROM   CookClarity.Reporting.V_CCHP_UMAuthorizationRequestStatusHistory_Fact aughx
            WHERE  aughx.AUTH_REQUEST_ID = aug.AUTH_REQUEST_ID
              AND  aughx.UM_STATUS_C    = 5                                      -- Denied
        )

    -- sla end

)

-- Minimal main SELECT so this parses as a complete statement.
-- Replace with the real final SELECT when you have it.
SELECT
    rd.[REFERRAL_ID],
    rd.[COVERAGE_ID],
    rd.[ENTRY_DATE],
    rd.[START_DATE],
    rd.[PRIORITY],
    rd.[REFERRAL_PROV_ID],
    rd.[REFERRING_PROV_ID],
    aug.AUTH_REQUEST_ID,
    aug.AUG_RECEIVED_DTTM,
    aug.LOB_ID,
    aug.RFL_TYPE_C
FROM ReferralDenials  rd
LEFT JOIN AugDenialSla aug
    ON aug.REFERRAL_ID = rd.[REFERRAL_ID];
