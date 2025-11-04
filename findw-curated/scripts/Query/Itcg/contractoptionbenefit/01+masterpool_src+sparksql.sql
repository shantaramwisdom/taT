

03+masterpool_parent_unit_id+sparksql.sql




09+finaldf+sparksql+overwrite.sql


select
distinct cast(a.view_row_create_day as date) as view_row_create_day,
cast(a.view_row_obsolete_day as date) as view_row_obsolete_day,
cast(a.view_row_effective_day as date) as view_row_effective_day,
cast(a.view_row_expiration_day as date) as view_row_expiration_day,
cast(a.policy_id as int) as policy_id,
cast(a.coverage_id as int) as coverage_id,
cast(a.policy_pool_id as int) as policy_pool_id,
cast(b.coverage_unit_id as int) as coverage_unit_id,
a.pool_type as ltcg_pool_type,
1 as ltcgpoolcount,
a.nh_indem_flg as nh_indemnity,
a.alf_indem_flg as al_indemnity,
a.hhc_indem_flg as hc_indemnity,
cast('{cycle_date}' as date) as cycle_date,
x.FILING_STATE as issuestate,
x.policy_no as policy_no,
null as max_greaterof_indic,
null as nh_lifetimemax,
null as nh_ltimunitmeasure,
null as nh_usedmaxdollars,
null as nh_usedmaxdays,
null as nh_lifetimemax1,
null as al_lifetimemax,
null as al_ltimunitmeasure,
null as al_usedmaxdollars,
null as al_usedmaxdays,
null as al_lifetimemax1,
null as hc_lifetimemax1,
null as hc_ltimunitmeasure1,
null as hc_lifetimemax2,
null as hc_ltimunitmeasure2,
null as hc_usedmaxdollars,
null as hc_usedmaxdays,
null as nh_elim,
null as nh_elimunitmeasure,
null as al_elim,
null as al_elimunitmeasure,
null as hc_elim,
null as hc_elimunitmeasure,
null as elim_dollarsused,
null as nhcurrdb,
null as alcurrdb,
null as hccurrdb,
null as profcurrdb,
null as vpu,
null as pooltype,
null as otherpool,
null as parent_unit_id,
null as shortdescription,
null as nh_lifemaxdays,
null as monthlyfac,
null as monthlyhc
from
(
select
distinct view_row_create_day,
view_row_obsolete_day,
view_row_effective_day,
view_row_expiration_day,
policy_id,
coverage_id,
policy_pool_id,
nh_indem_flg,
alf_indem_flg,
hhc_indem_flg,
pool_type
from
{source_database}.policy_pool_effective_history_DAILY_{cycledate}
where
'{cycle_date}' between cast(view_row_create_day as date) and cast(view_row_obsolete_day as date)
and add_months(cast('{cycle_date}' as date),1) = cast(view_row_effective_day as date)
and cast(view_row_effective_day as date) > cast('1901-01-01' as date)
) a
inner join (
select
distinct policy_id,
coverage_id,
policy_pool_id,
coverage_unit_id
from
{source_database}.policy_pool_limit_effective_history_DAILY_{cycledate}
where
'{cycle_date}' between cast( view_row_create_day as date) and cast(view_row_obsolete_day as date)) b on
a.policy_id = b.policy_id
and a.coverage_id = b.coverage_id
and a.policy_pool_id = b.policy_pool_id
join (
select
distinct policy_id,
POLICY_NO,
COVERAGE_ID,
min(cast(view_row_effective_day as date)) as view_row_effective_day,
max(cast(view_row_expiration_day as date)) as view_row_expiration_day,
FILING_STATE
from
{source_database}.policy_effective_history_DAILY_{cycledate}
where
'{cycle_date}' between cast(VIEW_ROW_CREATE_DAY as date) and cast(VIEW_ROW_OBSOLETE_DAY as date)
and policy_status_cd = 104
group by
POLICY_ID,
POLICY_NO,
COVERAGE_ID,
FILING_STATE) x on
a.POLICY_ID = x.POLICY_ID
and a.COVERAGE_ID = x.COVERAGE_ID
and cast(a.VIEW_ROW_EFFECTIVE_DAY as date) >= x.VIEW_ROW_EFFECTIVE_DAY
and cast(a.VIEW_ROW_EXPIRATION_DAY as date) <= x.VIEW_ROW_EXPIRATION_DAY
