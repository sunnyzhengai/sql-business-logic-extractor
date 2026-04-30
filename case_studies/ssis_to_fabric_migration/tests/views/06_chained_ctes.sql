-- Test: chained CTEs (CTE B reads from CTE A reads from base table). Columns
-- referenced via the second CTE alias should still flatten to the *base*
-- table in the manifest, not to the CTE name.
CREATE VIEW dbo.t06_chained_ctes AS
WITH RawReferrals AS (
    SELECT R.REFERRAL_ID, R.PATIENT_ID, R.STATUS_C
    FROM Clarity.dbo.REFERRAL R
),
DeniedReferrals AS (
    SELECT RR.REFERRAL_ID, RR.PATIENT_ID
    FROM RawReferrals RR
    WHERE RR.STATUS_C = 5
)
SELECT
    DR.REFERRAL_ID,
    DR.PATIENT_ID,
    P.PAT_NAME
FROM DeniedReferrals DR
INNER JOIN Clarity.dbo.PATIENT P ON P.PAT_ID = DR.PATIENT_ID;
