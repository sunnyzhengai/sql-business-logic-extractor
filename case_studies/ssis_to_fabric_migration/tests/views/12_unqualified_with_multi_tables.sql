-- Test: an unqualified column reference (no alias prefix) when multiple
-- tables are in scope. Without schema information, sqlglot can't pin the
-- column to a specific table. The manifest should fan out one row per
-- in-scope non-CTE table with confidence=low, so the ETL team sees the
-- column and can manually disambiguate, rather than seeing a row with
-- referenced_table=''.
CREATE VIEW dbo.t12_unqualified_with_multi_tables AS
SELECT
    [Member Address 1] = CSA.SUBSCR_ADDR,
    [Member Address 2] = CSA2.SUBSCR_ADDR,
    [Member City]      = SUBSCR_CITY,                 -- ← unqualified
    [Member Zip]       = SUBSCR_ZIP                   -- ← unqualified
FROM Clarity.dbo.SUBSCRIBER_ADDR CSA
LEFT JOIN Clarity.dbo.SUBSCRIBER_ADDR CSA2
    ON CSA.PAT_ID = CSA2.PAT_ID
   AND CSA2.ADDR_LINE_NUM = 2;
