with pkg_days as (
select
    package_id
    , updated_at
    , total_days
    , total_working_days
    , provider_service_types_name
    , providers_name
    , goal_diff_days
    , goal_status_days -- reponse var por tramo, NO es la response var final
    , goal_diff_working_days
    , goal_status_working_days
    , cast(
         pd.created_year_month_day
    as integer) date_int
--    
from db_stg_prod.package_days_by_provider pd
HAVING cast(
         pd.created_year_month_day
    as integer)  >= 20221001 and
    cast(
         pd.created_year_month_day
    as integer)  < 20221002
order by package_id
-- WHERE package_id = 62733167
),

customs_vendor as (
    -- Duplicate check: PASSED
    SELECT package_id, providers_name as customs_vendor FROM pkg_days
    WHERE provider_service_types_name = 'Custom Clearance'
),

distro_vendor as (
    -- Duplicate check: PASSED
    SELECT package_id, providers_name as um_vendor FROM pkg_days
    WHERE provider_service_types_name = 'Distribution'
),


tramo_overdue_mat as (
    -- Duplicate check: PASSED
    select
        package_id
        --, provider_service_types_name
        --, providers_name
        --, goal_status_working_days
        --- *********** OVERDUE Matrix ************
        , SUM(case
            WHEN 
                provider_service_types_name = 'Warehouse' 
                and goal_status_working_days = 'Overdue' 
            THEN 1 else 0 
        end) wh_overdue
        , SUM(case
            WHEN 
                provider_service_types_name = 'Transit' 
                and goal_status_working_days = 'Overdue' 
            THEN 1 else 0 
        end) transit_overdue
        , SUM(case
            WHEN 
                provider_service_types_name = 'Custom Clearance' 
                and goal_status_working_days = 'Overdue' 
            THEN 1 else 0 
        end) ad_overdue
        , SUM(case
            WHEN 
                provider_service_types_name = 'Distribution' 
                and goal_status_working_days = 'Overdue' 
            THEN 1 else 0 
        end) um_overdue
    from pkg_days
    -- test
    --where package_id = 49527690
    group by package_id
),

dias_por_tramo_mat as (
    -- Duplicate check: PASSED
    select
        package_id
        --, provider_service_types_name
        --, providers_name
        --, goal_status_working_days
        --- *********** OVERDUE Matrix ************
        , ROUND(SUM(case
            WHEN 
                provider_service_types_name = 'Warehouse' 
            THEN total_working_days else 0 
        end), 2) wh_working_days
        ,ROUND(SUM(case
            WHEN 
                provider_service_types_name = 'Transit'  
            THEN total_working_days else 0 
        end), 2) transit_working_days
        , ROUND(SUM(case
            WHEN 
                provider_service_types_name = 'Custom Clearance' 
            THEN total_working_days else 0 
        end), 2) customs_working_days
        , ROUND( SUM(case
            WHEN 
                provider_service_types_name = 'Distribution'
            THEN total_working_days else 0 
        end), 2) um_working_days
        , ROUND(SUM(total_working_days), 2) all_total_working_days
    from pkg_days
    -- test
    --where package_id = 49527690
    group by package_id
)



--/* main query
select m.*, cv.customs_vendor, dv.um_vendor
, dpt.wh_working_days ,  dpt.transit_working_days , dpt.customs_working_days 
, dpt.um_working_days, dpt.all_total_working_days
from tramo_overdue_mat m
left join customs_vendor cv on m.package_id = cv.package_id
left join distro_vendor dv on m.package_id = dv.package_id
left join dias_por_tramo_mat dpt on m.package_id = dpt.package_id

--*/

--select count(1) from tramo_overdue_mat

--Warehouse
--Transit
--Custom Clearance
--Distribution

/* duplicate test
SELECT package_id, count(1) from tramo_overdue_mat
group by package_id
having count(1)>1
*/