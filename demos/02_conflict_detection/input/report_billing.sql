-- Billing Report: Length of Stay (LOS) calculation
-- Uses midnight-to-midnight calendar days, from billing transactions

SELECT
    p.PAT_ID,
    p.PAT_NAME,
    t.TX_ID,
    t.SERVICE_DATE AS admission_date,
    t.POST_DATE AS discharge_date,
    -- LOS: Calendar days (cast to DATE first to ignore time component)
    DATEDIFF(DAY, CAST(t.SERVICE_DATE AS DATE), CAST(t.POST_DATE AS DATE)) AS length_of_stay,
    -- Readmission: Not tracked in billing report
    NULL AS readmission_flag,
    -- Patient Age: Based on service date
    DATEDIFF(YEAR, p.BIRTH_DATE, t.SERVICE_DATE) AS patient_age
FROM ARPB_TRANSACTIONS t
JOIN PATIENT p ON t.PATIENT_ID = p.PAT_ID
WHERE t.TX_TYPE_C = 1  -- Charges only
