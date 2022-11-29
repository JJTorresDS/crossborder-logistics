with pkg_details as (
select
    cast(
         pd.created_year_month_day
    as integer) y_m_d
    , SUBSTR(created_year_month_day,1,4) year
    , SUBSTR(created_year_month_day,5,2) month
    , package_id
    , package_zip_code_id
    , package_verified_origin_warehouse_code
    , package_package_state
    , package_flag_controlled
    , package_final_weight
    , package_flag_has_clockstop
    , package_delivery_duties
    , package_tracking_number
    , package_first_checkpoint_at
    , package_last_checkpoint_at
    , package_first_clockstop_at
    , cast(
        DATE_FORMAT(
        	from_unixtime(package_first_clockstop_at/1000),'%Y%m%d')
            as integer) first_clock_stop
    , package_value
    , package_weight
    , package_verified_weight
    , package_vol_weight
    , package_verified_vol_weight
    , package_delivery_route_id
    , package_ddp
    , client_name
    , service_id
    , service_code
    , origin_country_name
    , destination_country_name
    , marketplace_name
    , COALESCE(marketplace_name, client_name) mktplace_name_fixed
    , provider_service_wh_name
from packages_details pd
WHERE pd.package_package_state = 'Delivered'
and pd.package_flag_deleted_at <> 1
and pd.destination_country_name = 'Mexico'
-- and service_type_description = 'Priority'
and package_flag_controlled = 1 -- when the flag controlled equals 1 it follows the 'happy path'
-- and pe.created_year_month_day = '20200101'
),

pkg_events as (
select
    package_id
    , first_controlled_checkpoint_code
    , flag_hb
    , flag_wh
    , flag_td
    , flag_ar
    , flag_ad
    , flag_um
    , delivered_checkpoint_at
    , cast(
        DATE_FORMAT(
        	from_unixtime(delivered_checkpoint_at/1000),'%Y%m%d')
            as integer) delivered_at
--    
from package_events pe
WHERE current_stage = 'Distribution'
HAVING cast(
         pe.created_year_month_day
    as integer)  >= 20220101
),

e_details as (
    select
        package_id
        , controlled_working_days
        , wh_stalled_working_days
        , td_stalled_working_days
        , ar_stalled_working_days
        , ad_stalled_working_days
        , distribution_stalled_working_days
        , wh_total_working_days
        , td_total_working_days
        , hb_total_working_days
        , ar_total_working_days
        , ad_total_working_days
        , distribution_total_days
    from event_details ed
    HAVING cast(
             ed.created_year_month_day
        as integer)  >= 20220101
),

pkg_days as (
select
    package_id
    , updated_at
    , total_days
    , total_working_days
    , provider_service_types_name
    , providers_name
    , goal_diff_days
    , goal_status_days -- reponse var
    , goal_diff_working_days
    , goal_status_working_days
--    
from package_days_by_provider pd
WHERE provider_service_types_name = 'Distribution'
HAVING cast(
         pd.created_year_month_day
    as integer)  >= 20220101
-- WHERE package_id = 62733167
),

ymd_2022 as (
    
    SELECT pd.*
        ,  pe.first_controlled_checkpoint_code, pe.flag_hb, pe.flag_wh, pe.flag_td, pe.flag_ar
        , pe.flag_ad, pe.flag_um
        , pe.delivered_checkpoint_at, pe.delivered_at
    FROM pkg_details pd
    LEFT JOIN pkg_events pe on pd.package_id = pe.package_id
    LEFT JOIN e_details ed on pd.package_id = ed.package_id
    LEFT JOIN pkg_days pdp on pd.package_id = pdp.package_id
    WHERE y_m_d >= 20220101 and y_m_d < 20220301
)


select * from ymd_2022
/* QA query
select 
    origin_country_name
    , destination_country_name
    , count(1)
    , AVG(case 
     when goal_status_working_days = 'On Time' then 1 else 0 end) on_time_rate_wkdays
    , AVG(case 
     when goal_status_days = 'On Time' then 1 else 0 end) on_time_rate_days
    , AVG(goal_diff_working_days) avg_diff_wkdays
    , AVG(goal_diff_days) avg_diff_days
from ymd_2022
group by 1, 2
order by 4
*/
