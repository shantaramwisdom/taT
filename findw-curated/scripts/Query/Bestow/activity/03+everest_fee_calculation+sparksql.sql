WITH acq AS (
select *, SUM(1) over () as acq_cnt from (
select contractnumber, activityamounttype, activityamount, cycle_date
from {curated_database}.bestow_activity
where activitytypegroup = 'Fee'
and activityamounttype = 'Acquisition Expense'
and cast(cycle_date as date) < CAST('{cycle_date}' AS date))
),
maint AS (
select * from (
select contractnumber, cycle_date,
ROW_NUMBER() over (partition by contractnumber order by cycle_date desc) row_no
from {curated_database}.bestow_activity
where cast(cycle_date as date) between add_months(CAST('{cycle_date}' AS date), -13) and cast('{cycle_date}' as date) - 1
and activitytypegroup = 'Fee'
and activityamounttype = 'Maintenance Expense'
) where row_no = 1
)
select
src.*,
case
when acquisition_fee_flg = 'Y' then 20
when maintenance_fee_flg = 'Y' then 48
else NULL
end as activityamount,
case
when acquisition_fee_flg = 'Y' then 'ACQUISITION_EXPENSE'
when maintenance_fee_flg = 'Y' then 'MAINTENANCE_EXPENSE'
else NULL
end as activitysourcetransactioncode,
case
when acquisition_fee_flg = 'Y' then 'F'
when maintenance_fee_flg = 'Y' then 'R'
else NULL
end as activityfirstyearrenewalindicator
from (
select
evr_src.*,
COALESCE(acq_cnt.acq_cnt, 0) acq_cnt,
case
when COALESCE(acq_cnt.acq_cnt, 0) >= 50000 then 'N/A'
when acq.contractnumber is null then 'Y'
when acq.contractnumber is not null then 'N'
end as acquisition_check,
case when acquisition_check = 'Y' then ROW_NUMBER() OVER (ORDER BY evr_src.contractissuedate) + COALESCE(acq_cnt.acq_cnt, 0) end as acq_limit,
case when acq_limit < 50000 then 'Y' else 'N' end as acquisition_fee_flg,
case
when maint.contractnumber is not null
and datediff(evr_src.activityreportdate, maint.cycle_date) > 5 then 'N'
when datediff(evr_src.current_age, evr_src.prior_anniv, evr_src.activityreportdate) between 1 and 4
or datediff(DAY, evr_src.anniv, evr_src.activityreportdate) between 0 and 3
and (maint.contractnumber is null or datediff(evr_src.activityreportdate, maint.cycle_date) < 5)
then 'Y' else NULL end as maintenance_fee_flg
from everest_fee_src evr_src
left join acq on evr_src.contractnumber = acq.contractnumber
left join (select acq_cnt from acq limit 1) acq_cnt
left join maint on evr_src.contractnumber = maint.contractnumber
) src
where coalesce(src.acquisition_fee_flg, 'N') = 'Y' or coalesce(src.maintenance_fee_flg, 'N') = 'Y'
