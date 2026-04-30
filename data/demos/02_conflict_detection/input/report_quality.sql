-- Quality Report: Length of Stay (LOS) calculation
-- Uses ACCOUNT dates, includes same-day as LOS=1

SELECT
    p.PAT_ID,
    p.PAT_NAME,
    a.HSP_ACCOUNT_ID,
    a.ADM_DATE_TIME AS admission_date,
    a.DISCH_DATE_TIME AS discharge_date,
    -- LOS: Days between admission and discharge (account times) + 1 for same-day
    DATEDIFF(DAY, a.ADM_DATE_TIME, a.DISCH_DATE_TIME) + 1 AS length_of_stay,
    -- Readmission: Within 30 days of ADMISSION (not discharge!)
    CASE
        WHEN EXISTS (
            SELECT 1 FROM HSP_ACCOUNT a2
            WHERE a2.PAT_ID = a.PAT_ID
            AND a2.ADM_DATE_TIME > a.ADM_DATE_TIME
            AND DATEDIFF(DAY, a.ADM_DATE_TIME, a2.ADM_DATE_TIME) <= 30
        ) THEN 1
        ELSE 0
    END AS readmission_flag,
    -- Patient Age: At time of DISCHARGE (not admission!)
    DATEDIFF(YEAR, p.BIRTH_DATE, a.DISCH_DATE_TIME) AS patient_age
FROM HSP_ACCOUNT a
JOIN PATIENT p ON a.PAT_ID = p.PAT_ID
WHERE a.DISCH_DATE_TIME IS NOT NULL
