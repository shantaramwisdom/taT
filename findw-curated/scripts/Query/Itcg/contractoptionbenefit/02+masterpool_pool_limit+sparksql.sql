select
cast(view_row_create_day as date) as view_row_create_day,
cast(view_row_obsolete_day as date) as view_row_obsolete_day,
cast(view_row_effective_day as date) as view_row_effective_day,
cast(view_row_expiration_day as date) as view_row_expiration_day,
cast(policy_id as int) as policy_id,
cast(policy_pool_id as int) as policy_pool_id,
cast(coverage_id as int) as coverage_id,
cast(pool_limit_value as numeric(18, 2)) as pool_limit_value,
pool_limit_unit_of_measure_desc,
pool_limit_frequency_cd,
cast(POOL_LIFEMAX as numeric(28, 8)) as POOL_LIFEMAX,
cast(POOL_UNIT as numeric(18, 8)) as POOL_UNIT,
cast(pool_lifemax_remain_amt as numeric(18, 2)) as pool_lifemax_remain_amt,
cast(elimination_period as int) as elimination_period,
option_description,
option_type_cd,
OPTION_REFERENCE,
shared_benefit_cd,
cast('{cycle_date}' as date) as cycle_date,
OPTION_ID
from
{source_database}.policy_pool_limit_effective_history_DAILY_{cycledate}
where
'{cycle_date}' between cast(view_row_create_day as date) and cast(view_row_obsolete_day as date)
and (( option_id in (220575, 14806)
and pool_limit_frequency_cd = 1
and shared_benefit_cd = 1)
or (option_id in (220575, 14806)
and pool_limit_frequency_cd in (3, 10)
and shared_benefit_cd = 1)
or option_type_cd = 112
and pool_limit_frequency_cd = 1
and option_reference = 'NHMDB'
and shared_benefit_cd = 1
or option_id = 137
and pool_limit_frequency_cd = 1
and shared_benefit_cd = 1
or option_id in (10000, 27098, 18091)
and pool_limit_frequency_cd = 1
and shared_benefit_cd = 1
or option_type_cd = 113
and pool_limit_frequency_cd = 1
and option_reference in ('HHCMDB', 'HHCMDB-L2')
and shared_benefit_cd = 1
or option_type_cd = 113
and pool_limit_frequency_cd in (3, 10)
and option_reference = 'HHCMDB-L1'
and shared_benefit_cd = 1
or option_type_cd = 112
and pool_limit_frequency_cd in (3, 10)
and option_reference = 'NHMDB'
and shared_benefit_cd = 1
or option_type_cd = 137
and pool_limit_frequency_cd in (3, 10)
and shared_benefit_cd = 1
or option_id in (10000, 27098, 18091)
and pool_limit_frequency_cd in (3, 10)
and shared_benefit_cd = 1
or option_type_cd = 113
and pool_limit_frequency_cd in (3, 10)
and option_reference in ('HHCMDB', 'HHCMDB-L2')
and shared_benefit_cd = 1
or option_type_cd = 113
and pool_limit_frequency_cd in (3, 10)
and option_reference = 'HHCMDB-L1'
and shared_benefit_cd = 1
or option_reference = 'DAYSLIFE'
and shared_benefit_cd = 1
or option_id = 27182
or option_reference = 'GREATER_OF'
and shared_benefit_cd = 1
or option_type_cd = 112
and pool_limit_frequency_cd = 1
and shared_benefit_cd = 1
or option_type_cd = 112
and pool_limit_frequency_cd in (3, 10)
and shared_benefit_cd = 1
or option_type_cd = 137
and pool_limit_frequency_cd = 1
and shared_benefit_cd = 1
or option_id in (10000, 27098, 18091)
and pool_limit_frequency_cd = 1
and shared_benefit_cd = 1
or option_type_cd = 137
and pool_limit_frequency_cd in (3, 10)
and shared_benefit_cd = 1
or option_id in (10000, 27098, 18091)
and pool_limit_frequency_cd in (3, 10)
and shared_benefit_cd = 1
or option_type_cd = 113
and pool_limit_frequency_cd = 1
and shared_benefit_cd = 1
or option_type_cd = 113
and pool_limit_frequency_cd in (3, 10)
and shared_benefit_cd = 1)
