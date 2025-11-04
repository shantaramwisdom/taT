ltcg_masterpool_type_update_1 AS (
select
a.view_row_create_day,
a.view_row_obsolete_day,
a.view_row_effective_day,
a.view_row_expiration_day,
a.policy_id,
a.coverage_id,
a.policy_pool_id,
a.coverage_unit_id,
a.nh_indemnity,
a.al_indemnity,
a.hc_indemnity,
a.cycle_date,
a.issuestate,
a.policy_no,
a.nh_usedmaxdays,
a.hc_usedmaxdays,
a.elim_dollarsused,
a.vpu,
a.pooltype,
a.al_usedmaxdays,
a.otherpool,
a.parent_unit_id,
a.shortdescription,
a.ltcgpoolcount,
a.nh_elim,
a.nhcurrdb,
a.hc_elim,
a.hccurrdb,
a.profcurrdb,
a.nh_elimunitmeasure,
a.al_elimunitmeasure,
a.hc_elimunitmeasure,
a.monthlyfac,
a.al_elim,
a.alcurrdb,
a.monthlyhc,
a.nh_lifetimemax,
a.nh_ltimunitmeasure,
a.nh_lifemaxdays,
a.nh_usedmaxdollars,
a.al_lifetimemax,
a.al_ltimunitmeasure,
a.al_usedmaxdollars,
a.hc_lifetimemax1,
a.hc_ltimunitmeasure1,
a.hc_lifetimemax2,
a.hc_ltimunitmeasure2,
a.hc_usedmaxdollars,
a.max_greaterof_indic,
a.ltcg_pool_type_new_3 as ltcg_pool_type
from
(
select
a.*,
case
when ltcg_pool_type_new_2 = 'SHELL'
and ifnull(nhcurrdb, 0)>0
and ifnull(alcurrdb, 0)>0 then 'NH/ALF/HHC'
when ltcg_pool_type_new_2 = 'SHELL'
and ifnull(nhcurrdb, 0)>0
and ifnull(hccurrdb, 0)>0 then 'NH/HHC'
when ltcg_pool_type_new_2 = 'SHELL'
and ifnull(nhcurrdb, 0)>0
and ifnull(alcurrdb, 0)>0 then 'NH/ALF'
when ltcg_pool_type_new_2 = 'SHELL'
and ifnull(nhcurrdb, 0)>0 then 'NH'
when ltcg_pool_type_new_2 = 'SHELL'
and ifnull(alcurrdb, 0)>0 then 'ALF'
when ltcg_pool_type_new_2 = 'SHELL'
and ifnull(hccurrdb, 0)>0 then 'HHC'
end as ltcg_pool_type_new_3
from
(
select
a.*,
case
when a.ltcg_pool_type_new = 'NH/ALF'
and a.hccurrdb >0 then 'NH/ALF/HHC'
when a.ltcg_pool_type_new = 'NH'
and a.hccurrdb >0 then 'NH/HHC'
when a.ltcg_pool_type_new = 'ALF'
and a.hccurrdb >0 then 'ALF/HHC'
else a.ltcg_pool_type_new
end as ltcg_pool_type_new_2
from
(
select
a.*,
case
when a.ltcg_pool_type = 'NH'
and a.alcurrdb >0 then 'NH/ALF'
when a.ltcg_pool_type = 'NH'
and a.hccurrdb >0 then 'NH/HHC'
when a.ltcg_pool_type = 'ALF'
and a.hccurrdb >0 then 'ALF/HHC'
when a.ltcg_pool_type = 'HHC'
and a.alcurrdb >0 then 'ALF/HHC'
else a.ltcg_pool_type
end as ltcg_pool_type_new
from
masterpool_parent_unit_id a
) a
) a
) a
where
not(nhcurrdb is null
and alcurrdb is null
and hccurrdb is null)
and issuestate = 'KS'
and ltcg_pool_type = 'ALF'
and policy_pool_id = 27276
)
select
from_utc_timestamp(current_timestamp(), 'US/Central') as recorded_timestamp,
'{source_system_name}' as source_system_name,
a.view_row_create_day,
a.view_row_obsolete_day,
a.view_row_effective_day,
a.view_row_expiration_day,
a.policy_id,
a.coverage_id,
a.policy_pool_id,
a.coverage_unit_id,
a.ltcg_pool_type,
cast(a.ltcgpoolcount as int) as ltcgpoolcount,
a.max_greaterof_indic,
a.nh_lifetimemax,
a.nh_ltimunitmeasure,
a.nh_usedmaxdollars,
a.nh_usedmaxdays,
a.nh_indemnity,
a.al_lifetimemax,
a.al_ltimunitmeasure,
a.al_usedmaxdollars,
a.al_usedmaxdays,
a.al_indemnity,
cast(a.hc_lifetimemax1 as decimal(18, 2)) as hc_lifetimemax1,
a.hc_ltimunitmeasure1,
a.hc_lifetimemax2,
a.hc_ltimunitmeasure2,
a.hc_usedmaxdollars,
a.hc_usedmaxdays,
a.hc_indemnity,
a.nh_elim,
case
when a.nh_elimunitmeasure is null
and a.nhcurrdb > 0
and a.nh_elim > 4999 then 'Dollars'
when a.nh_elimunitmeasure is null
and a.nhcurrdb > 0
and a.nh_elim >= 0 then 'Days'
when a.nh_elimunitmeasure is null
and a.nhcurrdb > 0 then null
else a.nh_elimunitmeasure
end as nh_elimunitmeasure,
a.al_elim,
case
when a.al_elim > 4999
and a.al_elimunitmeasure is null
and a.alcurrdb > 0 then 'Dollars'
when a.al_elimunitmeasure is null
and a.alcurrdb > 0
and a.al_elim >= 0 then 'Days'
when a.al_elimunitmeasure is null
and a.alcurrdb > 0 then null
else a.al_elimunitmeasure
end as al_elimunitmeasure,
cast(a.hc_elim as int) as hc_elim,
case
when a.hc_elimunitmeasure is null
and a.hccurrdb > 0
and a.hc_elim > 4999 then 'Dollars'
when a.hc_elimunitmeasure is null
and a.hccurrdb > 0
and a.hc_elim >= 0 then 'Days'
when a.hc_elimunitmeasure is null
and a.hccurrdb > 0 then null
else a.hc_elimunitmeasure
end as hc_elimunitmeasure,
a.elim_dollarsused,
a.nhcurrdb,
a.alcurrdb,
a.hccurrdb,
a.profcurrdb,
a.vpu,
a.pooltype,
a.otherpool,
a.issuestate,
a.policy_no,
a.parent_unit_id,
a.shortdescription,
a.nh_lifemaxdays,
a.monthlyfac,
a.monthlyhc,
cast(a.cycle_date as date) as cycle_date,
cast('{batchid}' as int) as batchid
from
ltcg_masterpool_type_update_1 a

