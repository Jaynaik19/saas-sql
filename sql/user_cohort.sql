-- **Assumptions:**
-- - Cohort is by user joined date
-- - cohort can also be created by treating each reset of subscrption as new by reversing step. 4 where we created the start and end date for each and every event on subcription.


--0. STEPS TO GENERATE WEEKLY COHORT,
-- each setp is numbered in the query with an efffecient tile to explain the prcedure.
--1. get range min/max by week for all users that is possible
--2. generate week range from the range values
--3. get user data for all subcriptions.
--4. organize dates for all subcription based on start and end date.
--5. create a date range for each cohort: date_initialized and keep req. date range and remove no activity weeks.
--6. segment unique users by each  cohort and week
--7. rank each week for every cohort
--8. get agg cohort user count from sub data in 3.
--9. retention numbers using 7. and 8.



--1. get range min/max by week for all users that is possible
WITH range_values AS (

    SELECT 
        date_trunc('week', min(ss.date_initialized)) as minval,
        date_trunc('week', max(ss.date_updated)) as maxval
    FROM ss
    ),

    --2. generate week range from the range values
    week_range AS (
        SELECT generate_series(minval, maxval, '1 week'::interval)::date as week
        FROM range_values
    ),
    
    --3. get user data for all subcriptions.
    activity_data AS (

        SELECT
            ss.user_id,
            ss.id AS ssid,
            scs.id AS scsid,
            DATE_TRUNC('week', ss.date_initialized)::date AS date_initialized,
            DATE_TRUNC('week', scs.date_cancelled)::date AS date_cancelled,
            DATE_TRUNC('week', scs.date_resumed)::date AS date_resume,
            DATE_TRUNC('week', ss.date_paused)::date AS date_pause
        FROM ss
        LEFT JOIN scs 
        ON ss.id = scs.subscription_id
        WHERE ss.date_initialized IS NOT NULL 
        ORDER BY 2, 3
    ),
    
    -- 4. organize dates for all subcription based on start and end date.
    with_active_week AS (

        SELECT activity_data.user_id,
            activity_data.ssid,
            activity_data.scsid,
            activity_data.date_initialized,
            COALESCE(
                (
                    LAG(activity_data.date_resume,1)
                    OVER (PARTITION BY activity_data.user_id, activity_data.ssid
                    ORDER BY activity_data.ssid, activity_data.scsid)
                )
                , activity_data.date_initialized
            ) AS s_activity_date,
            activity_data.date_cancelled AS e_activity_date
        FROM activity_data
    ),
    
    --5. create a date range for each cohort: date_initialized and keep req. date range and remove no activity weeks.
    all_date_range AS (
        SELECT
            with_active_week.date_initialized AS cohort,
            week_range.week AS week,
            CASE
                WHEN week_range.week BETWEEN with_active_week.s_activity_date AND with_active_week.e_activity_date THEN with_active_week.user_id
            END active_user_id
        FROM with_active_week
        LEFT JOIN week_range ON with_active_week.date_initialized <= week_range.week
        WHERE (week_range.week BETWEEN with_active_week.s_activity_date AND with_active_week.e_activity_date) = True
        ORDER BY 1,2
    ),
    
    --6. segment unique users by each  cohort and week
    active_user_segment_by_week AS (
        SELECT
             all_date_range.cohort,
            all_date_range.week,
            COUNT(DISTINCT all_date_range.active_user_id) AS active_subs
        FROM all_date_range
        GROUP BY 1,2
        ORDER BY 1,2
    ),
        
    --7. rank each week for every cohort
    active_seg_date_range AS (
        SELECT
            active_user_segment_by_week.cohort,
            active_user_segment_by_week.week,
            active_user_segment_by_week.active_subs,
            (
                row_number()
                OVER (PARTITION BY active_user_segment_by_week.cohort
                ORDER BY active_user_segment_by_week.week ASC)
            ) AS week_number
        FROM active_user_segment_by_week
    ),
    
    --8. get agg cohort user count from sub data in 3.
    cohort_user_count AS (
        SELECT
            with_active_week.date_initialized AS cohort,
            count(distinct user_id) as cohort_total
        FROM with_active_week
        group by 1
    )
    
--9. retention numbers using 7. and 8.
SELECT
    active_seg_date_range.cohort,
    active_seg_date_range.week,
    active_seg_date_range.week_number,
    cohort_user_count.cohort_total,
    active_seg_date_range.active_subs,
    ROUND((active_seg_date_range.active_subs::decimal/cohort_user_count.cohort_total), 2) AS active_percent
FROM active_seg_date_range
LEFT JOIN cohort_user_count ON active_seg_date_range.cohort = cohort_user_count.cohort
ORDER BY 1,2
