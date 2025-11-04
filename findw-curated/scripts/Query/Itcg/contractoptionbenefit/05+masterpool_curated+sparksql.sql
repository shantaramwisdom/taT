select
POLICY_ID,
COVERAGE_ID,
COVERAGE_UNIT_ID,
'ALF' as contractoptionbenefittype,
alcurrdb as contractoptionbenefitdailybenefitamount,
al_elimunitmeasure as contractoptionbenefiteliminationunit,
cast(al_elim as int) as contractoptionbenefiteliminationvalue,
al_lifetimemax as contractoptionbenefitmaximumbefitallowed,
al_ltimunitmeasure as contractoptionbenefitmaximumbefitunit
from
{curated_database}.ltcg_masterpool_master_pool_ref
where
master_pool_ref.cycle_date = '{cycle_date}'
and cast(master_pool_ref.VIEW_ROW_CREATE_DAY as date) <= '{cycle_date}'
and ('{cycle_date}' <= cast(master_pool_ref.VIEW_ROW_OBSOLETE_DAY as date)
and cast(master_pool_ref.VIEW_ROW_EFFECTIVE_DAY as date) <= '{cycle_date}'
and ('{cycle_date}' < cast(master_pool_ref.VIEW_ROW_EXPIRATION_DAY as date))
and alcurrdb is not null
union all
select
POLICY_ID,
COVERAGE_ID,
COVERAGE_UNIT_ID,
'HHC' as ContractOptionBenefitType,
hccurrdb as contractoptionbenefitdailybenefitamount,
hc_elimunitmeasure as ContractOptionBenefiteliminationUnit,
cast(hc_elim as int) as contractoptionbenefiteliminationvalue,
hc_lifetimemax1 as contractoptionbenefitmaximumbenefitallowed,
hc_ltimunitmeasure1 as contractoptionbenefitmaximumbenefitunit
from
{curated_database}.ltcg_masterpool_master_pool_ref
where
master_pool_ref.cycle_date = '{cycle_date}'
and cast(master_pool_ref.VIEW_ROW_CREATE_DAY as date) <= '{cycle_date}'
and ('{cycle_date}' <= cast(master_pool_ref.VIEW_ROW_OBSOLETE_DAY as date)
and cast(master_pool_ref.VIEW_ROW_EFFECTIVE_DAY as date) <= '{cycle_date}'
and ('{cycle_date}' < cast(master_pool_ref.VIEW_ROW_EXPIRATION_DAY as date))
and hccurrdb is not null
union all
select
POLICY_ID,
COVERAGE_ID,
COVERAGE_UNIT_ID,
'NH' as ContractOptionBenefitType,
nhcurrdb as contractoptionbenefitdailybenefitamount,
nh_elimunitmeasure as ContractOptionBenefiteliminationUnit,
cast(nh_elim as int) as contractoptionbenefiteliminationvalue,
nh_lifetimemax as contractoptionbenefitmaximumbenefitallowed,
nh_ltimunitmeasure as contractoptionbenefitmaximumbenefitunit
from
{curated_database}.ltcg_masterpool_master_pool_ref
where
master_pool_ref.cycle_date = '{cycle_date}'
and cast(master_pool_ref.VIEW_ROW_CREATE_DAY as date) <= '{cycle_date}'
and ('{cycle_date}' <= cast(master_pool_ref.VIEW_ROW_OBSOLETE_DAY as date)
and cast(master_pool_ref.VIEW_ROW_EFFECTIVE_DAY as date) <= '{cycle_date}'
and ('{cycle_date}' < cast(master_pool_ref.VIEW_ROW_EXPIRATION_DAY as date))
and nhcurrdb is not null
