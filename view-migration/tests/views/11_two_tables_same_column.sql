-- Test: when two DIFFERENT tables have a column with the same name and the
-- SQL distinguishes them via aliases, BOTH should appear as separate rows
-- in the manifest. Confirms the dedup in test 10 is keyed on the resolved
-- table, not on the column name alone.
CREATE VIEW dbo.t11_two_tables_same_column AS
SELECT
    [Primary Address]   = ADDR1.STREET,
    [Secondary Address] = ADDR2.STREET
FROM Clarity.dbo.PRIMARY_ADDRESS ADDR1
LEFT JOIN Clarity.dbo.SECONDARY_ADDRESS ADDR2
    ON ADDR2.PAT_ID = ADDR1.PAT_ID;
