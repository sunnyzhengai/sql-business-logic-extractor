CREATE PROCEDURE [dbo].[sp_get_patient_visits]
    @pat_id INT,
    @start_date DATE = NULL,
    @end_date DATE = NULL,
    @status_filter VARCHAR(50) = 'Active'
AS
SELECT p.PAT_ID, p.PAT_NAME, e.CONTACT_DATE
FROM PATIENT p
INNER JOIN PAT_ENC e ON p.PAT_ID = e.PAT_ID
WHERE p.PAT_ID = @pat_id
