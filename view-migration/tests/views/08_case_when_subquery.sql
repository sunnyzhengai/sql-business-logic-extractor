-- Test: CASE expressions, scalar subqueries in SELECT, and IN-list filters
-- should all surface their referenced tables and columns correctly.
CREATE VIEW dbo.t08_case_when_subquery AS
SELECT
    R.REFERRAL_ID,
    CASE
        WHEN R.STATUS_C = 5 THEN 'Denied'
        WHEN R.STATUS_C IN (1, 2, 3) THEN 'Active'
        ELSE 'Other'
    END AS STATUS_LABEL,
    (SELECT COUNT(*) FROM Clarity.dbo.REFERRAL_HIST RH WHERE RH.REFERRAL_ID = R.REFERRAL_ID) AS HIST_COUNT
FROM Clarity.dbo.REFERRAL R
WHERE R.PATIENT_ID IN (SELECT PAT_ID FROM Clarity.dbo.PATIENT WHERE BIRTH_DATE > '2000-01-01');
