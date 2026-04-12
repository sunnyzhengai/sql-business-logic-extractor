/*
 * Complex Referral-to-Appointment Analytics Query
 *
 * Business Purpose:
 * Analyze referral conversion rates, appointment completion, and patient outcomes
 * across specialties and departments. Identify high-risk patients with multiple
 * no-shows and calculate specialty-level performance metrics.
 *
 * SQL Features Demonstrated:
 * - Nested CTEs (5 levels)
 * - Complex JOINs (INNER, LEFT, multiple tables)
 * - CASE expressions (simple and searched)
 * - Window functions (ROW_NUMBER, LAG, SUM OVER, RANK)
 * - Aggregations with GROUP BY and HAVING
 * - Subqueries in SELECT and WHERE
 * - Date arithmetic
 * - NULL handling (COALESCE, IS NULL)
 * - Multiple filter conditions
 *
 * Tables Used (from clarity_schema.yaml):
 * - REFERRAL, PATIENT, PAT_ENC, F_SCHED_APPT, CLARITY_DEP, CLARITY_SER
 * - ZC_RFL_STATUS, ZC_APPT_STATUS, ZC_SPECIALTY, ZC_CANCEL_REASON
 */

WITH
-- =============================================================================
-- CTE 1: Base referrals with status decoding
-- =============================================================================
base_referrals AS (
    SELECT
        r.REFERRAL_ID,
        r.PAT_ID,
        r.REFERRING_PROV_ID,
        r.PCP_PROV_ID,
        r.ENTRY_DATE AS referral_date,
        r.REFERRAL_STATUS_C,
        rs.NAME AS referral_status_name,
        r.PROV_SPEC_C AS specialty_code,
        sp.NAME AS specialty_name,
        r.RFL_CLASS_C,
        CASE r.RFL_CLASS_C
            WHEN 1 THEN 'Internal'
            WHEN 2 THEN 'Incoming'
            WHEN 3 THEN 'Outgoing'
            ELSE 'Unknown'
        END AS referral_class,
        CASE
            WHEN r.REFERRAL_STATUS_C IN (1, 2) THEN 'Active'
            WHEN r.REFERRAL_STATUS_C IN (4, 5) THEN 'Closed-Negative'
            WHEN r.REFERRAL_STATUS_C = 6 THEN 'Closed-Complete'
            ELSE 'Pending'
        END AS referral_category
    FROM REFERRAL r
    LEFT JOIN ZC_RFL_STATUS rs ON r.REFERRAL_STATUS_C = rs.RFL_STATUS_C
    LEFT JOIN ZC_SPECIALTY sp ON r.PROV_SPEC_C = sp.SPECIALTY_C
    WHERE r.ENTRY_DATE >= DATEADD(YEAR, -2, GETDATE())
      AND r.REFERRAL_STATUS_C IS NOT NULL
),

-- =============================================================================
-- CTE 2: Patient demographics and risk scoring
-- =============================================================================
patient_risk AS (
    SELECT
        p.PAT_ID,
        p.PAT_NAME,
        p.BIRTH_DATE,
        DATEDIFF(YEAR, p.BIRTH_DATE, GETDATE()) AS age_years,
        p.ZIP,
        -- Age-based risk category
        CASE
            WHEN DATEDIFF(YEAR, p.BIRTH_DATE, GETDATE()) < 18 THEN 'Pediatric'
            WHEN DATEDIFF(YEAR, p.BIRTH_DATE, GETDATE()) BETWEEN 18 AND 40 THEN 'Young Adult'
            WHEN DATEDIFF(YEAR, p.BIRTH_DATE, GETDATE()) BETWEEN 41 AND 65 THEN 'Middle Age'
            ELSE 'Senior'
        END AS age_category,
        -- Count of active problems (subquery)
        (
            SELECT COUNT(*)
            FROM PROBLEM_LIST pl
            WHERE pl.PAT_ID = p.PAT_ID
              AND pl.RESOLVED_DATE IS NULL
        ) AS active_problem_count
    FROM PATIENT p
),

-- =============================================================================
-- CTE 3: Appointment history with lag analysis for no-show patterns
-- =============================================================================
appointment_history AS (
    SELECT
        fa.PAT_ENC_CSN_ID,
        fa.PAT_ID,
        fa.REFERRAL_ID,
        fa.APPT_DTTM,
        fa.APPT_STATUS_C,
        ast.NAME AS appt_status_name,
        fa.DEPARTMENT_ID,
        dep.DEPARTMENT_NAME,
        dep.SPECIALTY AS dept_specialty,
        fa.PROV_ID,
        ser.PROV_NAME,
        fa.APPT_MADE_DTTM,
        fa.CHECKIN_DTTM,
        fa.CHECKOUT_DTTM,
        fa.CANCEL_REASON_C,
        cr.NAME AS cancel_reason,
        -- Days between scheduling and appointment
        DATEDIFF(DAY, fa.APPT_MADE_DTTM, fa.APPT_DTTM) AS lead_time_days,
        -- Appointment outcome classification
        CASE fa.APPT_STATUS_C
            WHEN 2 THEN 'Completed'
            WHEN 3 THEN 'Cancelled'
            WHEN 4 THEN 'No Show'
            WHEN 5 THEN 'Left Without Seen'
            WHEN 6 THEN 'Arrived'
            ELSE 'Scheduled'
        END AS appt_outcome,
        -- Wait time in minutes (check-in to checkout)
        CASE
            WHEN fa.CHECKIN_DTTM IS NOT NULL AND fa.CHECKOUT_DTTM IS NOT NULL
            THEN DATEDIFF(MINUTE, fa.CHECKIN_DTTM, fa.CHECKOUT_DTTM)
            ELSE NULL
        END AS visit_duration_minutes,
        -- Previous appointment status (lag window function)
        LAG(fa.APPT_STATUS_C, 1) OVER (
            PARTITION BY fa.PAT_ID
            ORDER BY fa.APPT_DTTM
        ) AS prev_appt_status,
        -- Appointment sequence number per patient
        ROW_NUMBER() OVER (
            PARTITION BY fa.PAT_ID
            ORDER BY fa.APPT_DTTM
        ) AS patient_appt_seq,
        -- Running count of no-shows per patient
        SUM(CASE WHEN fa.APPT_STATUS_C = 4 THEN 1 ELSE 0 END) OVER (
            PARTITION BY fa.PAT_ID
            ORDER BY fa.APPT_DTTM
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_no_shows
    FROM F_SCHED_APPT fa
    LEFT JOIN ZC_APPT_STATUS ast ON fa.APPT_STATUS_C = ast.APPT_STATUS_C
    LEFT JOIN CLARITY_DEP dep ON fa.DEPARTMENT_ID = dep.DEPARTMENT_ID
    LEFT JOIN CLARITY_SER ser ON fa.PROV_ID = ser.PROV_ID
    LEFT JOIN ZC_CANCEL_REASON cr ON fa.CANCEL_REASON_C = cr.CANCEL_REASON_C
    WHERE fa.APPT_DTTM >= DATEADD(YEAR, -2, GETDATE())
),

-- =============================================================================
-- CTE 4: Referral-to-appointment matching with time-to-first-appointment
-- =============================================================================
referral_appointments AS (
    SELECT
        br.REFERRAL_ID,
        br.PAT_ID,
        br.referral_date,
        br.referral_status_name,
        br.specialty_name,
        br.referral_class,
        br.referral_category,
        pr.PAT_NAME,
        pr.age_years,
        pr.age_category,
        pr.active_problem_count,
        ah.PAT_ENC_CSN_ID,
        ah.APPT_DTTM,
        ah.appt_outcome,
        ah.DEPARTMENT_NAME,
        ah.PROV_NAME,
        ah.lead_time_days,
        ah.visit_duration_minutes,
        ah.cumulative_no_shows,
        -- Days from referral to appointment
        DATEDIFF(DAY, br.referral_date, ah.APPT_DTTM) AS days_to_appointment,
        -- Flag if this is the first appointment for this referral
        ROW_NUMBER() OVER (
            PARTITION BY br.REFERRAL_ID
            ORDER BY ah.APPT_DTTM
        ) AS appt_sequence_for_referral,
        -- Rank appointments by completion status
        RANK() OVER (
            PARTITION BY br.REFERRAL_ID
            ORDER BY CASE ah.APPT_STATUS_C WHEN 2 THEN 0 ELSE 1 END, ah.APPT_DTTM
        ) AS completion_rank
    FROM base_referrals br
    INNER JOIN patient_risk pr ON br.PAT_ID = pr.PAT_ID
    LEFT JOIN appointment_history ah ON br.REFERRAL_ID = ah.REFERRAL_ID
),

-- =============================================================================
-- CTE 5: Specialty-level aggregations
-- =============================================================================
specialty_metrics AS (
    SELECT
        specialty_name,
        COUNT(DISTINCT REFERRAL_ID) AS total_referrals,
        COUNT(DISTINCT PAT_ID) AS unique_patients,
        COUNT(DISTINCT CASE WHEN PAT_ENC_CSN_ID IS NOT NULL THEN REFERRAL_ID END) AS referrals_with_appointments,
        COUNT(DISTINCT CASE WHEN appt_outcome = 'Completed' THEN REFERRAL_ID END) AS referrals_completed,
        AVG(CAST(days_to_appointment AS FLOAT)) AS avg_days_to_first_appt,
        AVG(CAST(lead_time_days AS FLOAT)) AS avg_scheduling_lead_time,
        AVG(CAST(visit_duration_minutes AS FLOAT)) AS avg_visit_duration,
        -- Conversion rate
        CAST(COUNT(DISTINCT CASE WHEN PAT_ENC_CSN_ID IS NOT NULL THEN REFERRAL_ID END) AS FLOAT) /
            NULLIF(COUNT(DISTINCT REFERRAL_ID), 0) * 100 AS conversion_rate_pct,
        -- Completion rate (of those with appointments)
        CAST(COUNT(DISTINCT CASE WHEN appt_outcome = 'Completed' THEN REFERRAL_ID END) AS FLOAT) /
            NULLIF(COUNT(DISTINCT CASE WHEN PAT_ENC_CSN_ID IS NOT NULL THEN REFERRAL_ID END), 0) * 100 AS completion_rate_pct
    FROM referral_appointments
    WHERE appt_sequence_for_referral = 1 OR appt_sequence_for_referral IS NULL
    GROUP BY specialty_name
    HAVING COUNT(DISTINCT REFERRAL_ID) >= 10
)

-- =============================================================================
-- Final SELECT: Detailed referral analytics with specialty benchmarks
-- =============================================================================
SELECT
    ra.REFERRAL_ID,
    ra.PAT_ID,
    ra.PAT_NAME,
    ra.age_years,
    ra.age_category,
    ra.active_problem_count,
    ra.referral_date,
    ra.referral_status_name,
    ra.specialty_name,
    ra.referral_class,
    ra.referral_category,
    ra.PAT_ENC_CSN_ID AS first_appt_csn,
    ra.APPT_DTTM AS first_appt_date,
    ra.appt_outcome AS first_appt_outcome,
    ra.DEPARTMENT_NAME,
    ra.PROV_NAME AS first_appt_provider,
    ra.days_to_appointment,
    ra.lead_time_days,
    ra.visit_duration_minutes,
    ra.cumulative_no_shows,
    -- Compare to specialty average
    sm.avg_days_to_first_appt AS specialty_avg_days,
    COALESCE(ra.days_to_appointment, 0) - COALESCE(sm.avg_days_to_first_appt, 0) AS days_vs_specialty_avg,
    sm.conversion_rate_pct AS specialty_conversion_rate,
    sm.completion_rate_pct AS specialty_completion_rate,
    -- Risk flags
    CASE
        WHEN ra.cumulative_no_shows >= 3 THEN 'High Risk - Multiple No Shows'
        WHEN ra.cumulative_no_shows >= 1 AND ra.age_category = 'Senior' THEN 'Medium Risk - Senior with No Show History'
        WHEN ra.active_problem_count >= 5 THEN 'Medium Risk - Complex Patient'
        WHEN ra.days_to_appointment > sm.avg_days_to_first_appt * 2 THEN 'Medium Risk - Long Wait Time'
        ELSE 'Standard'
    END AS patient_risk_flag,
    -- Referral success indicator
    CASE
        WHEN ra.appt_outcome = 'Completed' THEN 1
        WHEN ra.appt_outcome IN ('Cancelled', 'No Show') THEN 0
        WHEN ra.PAT_ENC_CSN_ID IS NULL AND ra.referral_category = 'Active' THEN NULL  -- Still pending
        ELSE 0
    END AS referral_success_flag
FROM referral_appointments ra
LEFT JOIN specialty_metrics sm ON ra.specialty_name = sm.specialty_name
WHERE ra.appt_sequence_for_referral = 1
   OR ra.appt_sequence_for_referral IS NULL
ORDER BY
    ra.specialty_name,
    ra.referral_date DESC;
