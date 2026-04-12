-- Query B: Direct calculation without CTEs
-- Should match Query A at L5 level despite different structure

SELECT
    p.PAT_ID,
    p.PAT_NAME,
    DATEDIFF(YEAR, p.BIRTH_DATE, GETDATE()) AS age_years,
    CASE
        WHEN DATEDIFF(YEAR, p.BIRTH_DATE, GETDATE()) < 18 THEN 'Pediatric'
        WHEN DATEDIFF(YEAR, p.BIRTH_DATE, GETDATE()) BETWEEN 18 AND 65 THEN 'Adult'
        ELSE 'Senior'
    END AS age_category,
    r.REFERRAL_ID,
    r.ENTRY_DATE AS referral_date,
    DATEDIFF(DAY, r.ENTRY_DATE, GETDATE()) AS days_since_referral
FROM PATIENT p
JOIN REFERRAL r ON p.PAT_ID = r.PAT_ID
WHERE r.REFERRAL_STATUS_C = 1
