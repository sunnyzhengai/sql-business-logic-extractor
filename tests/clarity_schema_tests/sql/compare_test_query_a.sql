-- Query A: Uses CTEs for patient age calculation
-- Should match Query B at L5 level despite different structure

WITH patient_demographics AS (
    SELECT
        PAT_ID,
        PAT_NAME,
        BIRTH_DATE,
        DATEDIFF(YEAR, BIRTH_DATE, GETDATE()) AS age_years
    FROM PATIENT
),
patient_categories AS (
    SELECT
        PAT_ID,
        PAT_NAME,
        age_years,
        CASE
            WHEN age_years < 18 THEN 'Pediatric'
            WHEN age_years BETWEEN 18 AND 65 THEN 'Adult'
            ELSE 'Senior'
        END AS age_category
    FROM patient_demographics
)
SELECT
    pc.PAT_ID,
    pc.PAT_NAME,
    pc.age_years,
    pc.age_category,
    r.REFERRAL_ID,
    r.ENTRY_DATE AS referral_date,
    DATEDIFF(DAY, r.ENTRY_DATE, GETDATE()) AS days_since_referral
FROM patient_categories pc
JOIN REFERRAL r ON pc.PAT_ID = r.PAT_ID
WHERE r.REFERRAL_STATUS_C = 1
