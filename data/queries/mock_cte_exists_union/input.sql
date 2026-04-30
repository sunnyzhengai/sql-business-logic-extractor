-- Mock: CTE with passthrough columns + WHERE EXISTS (UNION ALL of two subselects)
-- Mimics the structure the user is testing at work with a 900+ line query.
-- Exercises: passthrough CTE, correlated EXISTS, UNION ALL inside EXISTS,
-- two-table correlation, filter clauses in each branch.

WITH ActivePatients AS (
    SELECT
        p.PAT_ID,
        p.PAT_NAME,
        p.BIRTH_DATE,
        p.SEX_C
    FROM PATIENT p
    WHERE EXISTS (
        SELECT 1
        FROM PAT_ENC pe
        WHERE pe.PAT_ID = p.PAT_ID
          AND pe.CONTACT_DATE >= DATEADD(YEAR, -1, GETDATE())
        UNION ALL
        SELECT 1
        FROM PROBLEM_LIST pl
        WHERE pl.PAT_ID = p.PAT_ID
          AND pl.RESOLVED_DATE IS NULL
    )
)
SELECT
    ap.PAT_ID,
    ap.PAT_NAME,
    ap.BIRTH_DATE,
    ap.SEX_C
FROM ActivePatients ap
WHERE ap.BIRTH_DATE < DATEADD(YEAR, -18, GETDATE());
