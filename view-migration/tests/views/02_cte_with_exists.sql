-- Test: CTE references must be filtered out of the manifest, but CTE-internal
-- columns must be flattened back to the underlying base tables. EXISTS
-- subqueries should also surface their referenced tables/columns.
CREATE VIEW dbo.t02_cte_with_exists AS
WITH ActiveReferrals AS (
    SELECT R.REFERRAL_ID, R.STATUS_C, R.PATIENT_ID
    FROM Clarity.dbo.REFERRAL R
    WHERE R.STATUS_C = 1
)
SELECT
    AR.REFERRAL_ID,
    AR.PATIENT_ID,
    P.PAT_NAME
FROM ActiveReferrals AR
INNER JOIN Clarity.dbo.PATIENT P ON P.PAT_ID = AR.PATIENT_ID
WHERE EXISTS (
    SELECT 1
    FROM Clarity.dbo.REFERRAL_HIST RH
    WHERE RH.REFERRAL_ID = AR.REFERRAL_ID
      AND RH.NEW_STATUS_C = 5
);
