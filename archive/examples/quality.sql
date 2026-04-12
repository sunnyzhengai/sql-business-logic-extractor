-- Quality Dashboard
WITH encounters AS (
    SELECT e.PAT_ID, e.PAT_ENC_CSN_ID,
           e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME,
           DATEDIFF(DAY, e.HOSP_ADMSN_TIME, e.HOSP_DISCH_TIME) AS los_days,
           CASE
               WHEN e.DISCH_DISPOSITION_C = 1 THEN 'Home'
               WHEN e.DISCH_DISPOSITION_C = 2 THEN 'Transfer'
               WHEN e.DISCH_DISPOSITION_C = 20 THEN 'Expired'
               ELSE 'Other'
           END AS disposition
    FROM PAT_ENC_HSP e
    WHERE e.HOSP_DISCH_TIME IS NOT NULL AND e.ADT_PAT_CLASS_C = 1
)
SELECT COUNT(*) AS total,
       AVG(los_days) AS mean_los,
       COUNT(CASE WHEN disposition = 'Expired' THEN 1 END) AS mortality_count
FROM encounters
