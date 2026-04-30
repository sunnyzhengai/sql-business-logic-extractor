-- Test: multiple JOINs (INNER, LEFT) plus SELECT * should produce a table-level
-- row for each joined table, AND not lose anything just because the query
-- selects a wildcard.
CREATE VIEW dbo.t05_multi_join_select_star AS
SELECT
    R.*,
    P.PAT_NAME,
    H.ADMIT_DATE
FROM Clarity.dbo.REFERRAL R
INNER JOIN Clarity.dbo.PATIENT P ON P.PAT_ID = R.PATIENT_ID
LEFT  JOIN dbo.HSP_ACCOUNT H ON H.PAT_ID = R.PATIENT_ID
LEFT  JOIN CookClarity.Reporting.V_AUTH_FACT AUTH ON AUTH.REFERRAL_ID = R.REFERRAL_ID;
