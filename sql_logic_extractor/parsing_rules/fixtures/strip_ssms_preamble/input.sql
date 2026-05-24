USE [Reporting]
GO
/****** Object:  View [Reporting].[V_CCHCS_DXP_HP_Mychart_PBI]    Script Date: 5/23/2026 10:05:28 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO

CREATE   VIEW [Reporting].[V_CCHCS_DXP_HP_Mychart_PBI]
AS

WITH YearMonth AS (
    SELECT YearMonth = CONVERT(CHAR(6), DD.CALENDAR_DATE, 112)
    FROM DateDimension DD
)
SELECT * FROM YearMonth
