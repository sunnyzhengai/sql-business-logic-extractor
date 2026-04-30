-- Finance Report: Length of Stay (LOS) calculation
-- Uses HOSPITAL discharge times, excludes same-day discharges

SELECT
    p.PAT_ID,
    p.PAT_NAME,
    h.PAT_ENC_CSN_ID,
    h.HOSP_ADMSN_TIME AS admission_date,
    h.HOSP_DISCH_TIME AS discharge_date,
    -- LOS: Days between admission and discharge (hospital times)
    DATEDIFF(DAY, h.HOSP_ADMSN_TIME, h.HOSP_DISCH_TIME) AS length_of_stay,
    -- Readmission: Within 30 days of DISCHARGE
    CASE
        WHEN EXISTS (
            SELECT 1 FROM PAT_ENC_HSP h2
            WHERE h2.PAT_ID = h.PAT_ID
            AND h2.HOSP_ADMSN_TIME > h.HOSP_DISCH_TIME
            AND DATEDIFF(DAY, h.HOSP_DISCH_TIME, h2.HOSP_ADMSN_TIME) <= 30
        ) THEN 1
        ELSE 0
    END AS readmission_flag,
    -- Patient Age: At time of ADMISSION
    DATEDIFF(YEAR, p.BIRTH_DATE, h.HOSP_ADMSN_TIME) AS patient_age
FROM PAT_ENC_HSP h
JOIN PATIENT p ON h.PAT_ID = p.PAT_ID
WHERE h.HOSP_DISCH_TIME IS NOT NULL
  AND DATEDIFF(DAY, h.HOSP_ADMSN_TIME, h.HOSP_DISCH_TIME) > 0  -- Exclude same-day
