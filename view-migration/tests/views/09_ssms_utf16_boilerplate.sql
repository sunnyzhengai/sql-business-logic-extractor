-- Marker file: this view exists in *two* forms in the test suite —
--   - this UTF-8 source (here) for human review
--   - a UTF-16-LE-with-BOM binary version generated at test time by run_tests.py
-- so the SSMS encoding handler is exercised against a real BOM'd file
-- without committing binary fixtures into git.
USE [Clarity]
GO
/****** Object:  View [dbo].[t09_ssms_utf16_boilerplate]    Script Date: 4/28/2026 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW dbo.t09_ssms_utf16_boilerplate AS
SELECT
    P.PAT_ID,
    P.PAT_NAME,
    P.BIRTH_DATE
FROM Clarity.dbo.PATIENT P
WHERE P.STATUS_C = 1;
GO
