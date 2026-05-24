CREATE   VIEW [Reporting].[V_CCHCS_DXP_HP_Mychart_PBI]
AS

WITH YearMonth AS (
    SELECT YearMonth = CONVERT(CHAR(6), DD.CALENDAR_DATE, 112)
    FROM DateDimension DD
)
SELECT * FROM YearMonth
