-- Test: 3-part names (database.schema.table) and 2-part names should both
-- be captured with the database/schema fields populated correctly.
CREATE VIEW dbo.t01_basic_three_part AS
SELECT
    P.PAT_ID,
    P.PAT_NAME,
    H.ADMIT_DATE
FROM Clarity.dbo.PATIENT P
INNER JOIN dbo.HSP_ACCOUNT H
    ON H.PAT_ID = P.PAT_ID;
