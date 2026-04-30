-- Operations Report: Length of Stay (LOS) calculation
-- Uses HOURS instead of days, from encounter table

SELECT
    p.PAT_ID,
    p.PAT_NAME,
    e.PAT_ENC_CSN_ID,
    e.APPT_TIME AS admission_date,
    e.CONTACT_DATE AS discharge_date,
    -- LOS: HOURS between appointment and contact (not days!)
    DATEDIFF(HOUR, e.APPT_TIME, e.CONTACT_DATE) AS length_of_stay,
    -- Readmission: Within 7 days (not 30!) using encounter dates
    CASE
        WHEN EXISTS (
            SELECT 1 FROM PAT_ENC e2
            WHERE e2.PAT_ID = e.PAT_ID
            AND e2.APPT_TIME > e.CONTACT_DATE
            AND DATEDIFF(DAY, e.CONTACT_DATE, e2.APPT_TIME) <= 7
        ) THEN 1
        ELSE 0
    END AS readmission_flag,
    -- Patient Age: Current age (not at admission or discharge!)
    DATEDIFF(YEAR, p.BIRTH_DATE, GETDATE()) AS patient_age
FROM PAT_ENC e
JOIN PATIENT p ON e.PAT_ID = p.PAT_ID
WHERE e.ENC_TYPE_C = 3  -- Hospital encounters only
