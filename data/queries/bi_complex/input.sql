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

-- Sla Start --
, AugHistorySla AS (
    SELECT
        aut.UM_AUTH_REQUEST_ID,
        authx.CHANGED_BY_USER_ID,
        [DENIAL_ROW] = row_number() OVER (
            PARTITION BY aut.UM_AUTH_REQUEST_ID
            ORDER BY IIF(authx.UM_STATUS = 'Denied', authx.CHANGED_DTTM, '99991231')
        ),
        [DENIAL_DATE]   = IIF(authx.UM_STATUS = 'Denied', authx.CHANGED_DTTM, NULL),
        [DENIAL_REASON] = IIF(authx.UM_STATUS = 'Denied', authx.UM_DENIED_RSN, NULL),
        [RFI_ROW] = row_number() OVER (
            PARTITION BY aut.UM_AUTH_REQUEST_ID
            ORDER BY IIF(authx.UM_PENDING_RSN = 'HP Request for Information', authx.CHANGED_DTTM, '99991231')
        ),
        [RFI_DATE] = IIF(authx.UM_PENDING_RSN = 'HP Request for Information', authx.CHANGED_DTTM, NULL),
        authx.UM_STATUS,
        authx.CHANGED_DTTM,
        authx.UM_DENIED_RSN,
        authx.UM_PENDING_RSN
    FROM AugDenialSla augd
    JOIN CookClarity.Reporting.V_CCHP_UMAuthorization_Fact aut
        ON aut.UM_AUTH_REQUEST_ID = augd.AUTH_REQUEST_ID
    JOIN CookClarity.Reporting.V_CCHP_UMAuthorizationHistory_Fact authx
        ON aut.AUTH_ID = authx.AUTH_ID
    WHERE
        authx.UM_STATUS = 'Denied'
        OR (
            authx.UM_STATUS = 'Pending Review'
            AND authx.UM_PENDING_RSN = 'HP Request for Information'
        )
)

, ReferralHistoryItemValues AS (

    SELECT

        RFLH.REFERRAL_ID,
        RFLH.HX_USER_ID,

        RFLHA.ACTION_DTTM,
        RFLHA.GROUP_LINE,

        RFLHIC.ITEM_CHANGE,

        RFLHNV.NEW_VALUE_EXTERNAL

    FROM

        ReferralDenials RFLD

        JOIN Clarity.dbo.REFERRAL_HISTORY RFLH
            ON RFLH.REFERRAL_ID = RFLD.REFERRAL_ID

        JOIN Clarity.dbo.RFL_HX_ACT RFLHA
            ON  RFLHA.REFERRAL_ID = RFLH.REFERRAL_ID
            AND RFLHA.GROUP_LINE  = RFLH.LINE

        JOIN Clarity.dbo.RFL_HX_ITEM_CHANGE RFLHIC
            ON  RFLHIC.REFERRAL_ID  = RFLHA.REFERRAL_ID
            AND RFLHIC.GROUP_LINE   = RFLHA.GROUP_LINE
            AND RFLHIC.VALUE_LINE   = RFLHA.VALUE_LINE
            AND RFLHIC.ITEM_CHANGE IN (
                50,      -- Referral Status
                18007,   -- Denial Reason
                18003,   -- Pending Reason
                2081,    -- Bed Day Status
                2080     -- Bed Day Denied Reason
            )

        JOIN Clarity.dbo.RFL_HX_NEW_VAL RFLHNV
            ON  RFLHNV.REFERRAL_ID  = RFLHA.REFERRAL_ID
            AND RFLHNV.GROUP_LINE   = RFLHA.GROUP_LINE
            AND RFLHNV.VALUE_LINE   = RFLHA.VALUE_LINE
            AND (
                (
                    RFLHIC.ITEM_CHANGE = 18007
                    AND RFLHNV.NEW_VAL IS NOT NULL
                )
                OR (
                    RFLHIC.ITEM_CHANGE = 18003
                    AND RFLHNV.NEW_VAL = 1640000010                                  -- Request for Information
                )
                OR (
                    RFLHIC.ITEM_CHANGE = 50
                    AND RFLHNV.NEW_VAL = '5'                                         -- Denied
                )
                OR (
                    RFLHIC.ITEM_CHANGE = 2081
                    AND RFLHNV.NEW_VAL = '2'                                         -- Denied
                )
                OR (
                    RFLHIC.ITEM_CHANGE = 2080
                    AND RFLHNV.NEW_VAL IS NOT NULL
                )
            )

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
    aug.RFL_TYPE_C,
    rhiv.ITEM_CHANGE,
    rhiv.NEW_VALUE_EXTERNAL,
    rhiv.ACTION_DTTM,
    ahs.DENIAL_ROW,
    ahs.DENIAL_DATE,
    ahs.DENIAL_REASON,
    ahs.RFI_ROW,
    ahs.RFI_DATE
FROM ReferralDenials  rd
LEFT JOIN AugDenialSla aug
    ON aug.REFERRAL_ID = rd.[REFERRAL_ID]
LEFT JOIN ReferralHistoryItemValues rhiv
    ON rhiv.REFERRAL_ID = rd.[REFERRAL_ID]
LEFT JOIN AugHistorySla ahs
    ON ahs.UM_AUTH_REQUEST_ID = aug.AUTH_REQUEST_ID;
