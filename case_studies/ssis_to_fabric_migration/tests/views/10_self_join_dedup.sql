-- Test: a self-join with two aliases for the SAME underlying table should
-- produce ONE manifest row per distinct (db, schema, table, column) tuple,
-- even though the SQL references the column under two different aliases.
-- Migration semantics: renaming the underlying column once covers both.
CREATE VIEW dbo.t10_self_join_dedup AS
SELECT
    [Member Address 1] = CSA.SUBSCR_ADDR,
    [Member Address 2] = CSA2.SUBSCR_ADDR
FROM Clarity.dbo.SUBSCRIBER_ADDR CSA
LEFT JOIN Clarity.dbo.SUBSCRIBER_ADDR CSA2
    ON CSA.PAT_ID = CSA2.PAT_ID
   AND CSA2.ADDR_LINE_NUM = 2;
