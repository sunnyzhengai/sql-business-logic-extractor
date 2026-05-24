USE [MyHealthcareDB]
GO
/****** Object:  View [dbo].[vw_mychart_patient_summary]    Script Date: 5/24/2026 9:15:42 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO

CREATE VIEW [dbo].[vw_mychart_patient_summary] AS
SELECT pat_id, pat_name
FROM PATIENT
WHERE status_c = 2
