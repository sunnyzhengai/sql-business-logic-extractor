CREATE VIEW [dbo].[vw_mychart_patient_summary] AS
SELECT pat_id, pat_name
FROM PATIENT
WHERE status_c = 2
