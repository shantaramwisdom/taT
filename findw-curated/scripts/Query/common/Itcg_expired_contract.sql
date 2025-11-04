with expired as (
    select policy_no, policy_id
    from {source_database}.policy_effective_history_daily_{cycledate}
    where cast('{cycle_date}' as date) between cast(view_row_create_day as date)
    and cast(view_row_obsolete_day as date)
    and cast('{cycle_date}' as date) between cast(view_row_effective_day as date)
    and cast(view_row_expiration_day as date)
    and
    cast(policy_status_dt as date) < cast('{two_year_from_cycle_date}' as date)
    and (
            policy_status_cd = '105'
            and cast(coverage_expiration_dt as date) < cast('{seven_year_from_cycle_date}' as date)
        or
            policy_status_cd = '106'
            and cast(original_effective_dt as date) < cast('{seven_year_from_cycle_date}' as date)
        )
),
not_expired as (
    select policy_no, policy_id
    from {source_database}.policy_effective_history_daily_{cycledate}
    where cast('{cycle_date}' as date) between cast(view_row_create_day as date)
    and cast(view_row_obsolete_day as date)
    and cast('{cycle_date}' as date) between cast(view_row_effective_day as date)
    and cast(view_row_expiration_day as date)
    and (
        policy_status_cd in ('104')
        or (policy_status_cd = '105'
            and cast(policy_status_dt as date) >= cast('{two_year_from_cycle_date}' as date))
        or (policy_status_cd = '106'
            and cast(policy_status_dt as date) >= cast('{two_year_from_cycle_date}' as date))
        )
),
linkage as (
    select prim_policy_id,
           sec_policy_id
    from {source_database}.policy_linkage_effective_history_daily_{cycledate}
    where cast('{cycle_date}' as date) between cast(view_row_create_day as date)
    and cast(view_row_obsolete_day as date)
    and cast('{cycle_date}' as date) between cast(view_row_effective_day as date)
    and cast(view_row_expiration_day as date)
    group by 1,
             2
),
ignore_linkage as (
    select policy_no,
           prim_policy_id as policy_id,
           sec_policy_id as related_policy_id
    from linkage
         join not_expired b on prim_policy_id = policy_id
    union
    select policy_no,
           sec_policy_id as policy_id,
           prim_policy_id as related_policy_id
    from linkage
         join not_expired on sec_policy_id = policy_id
)
select *
from expired a
     left anti join ignore_linkage b on a.policy_id = b.related_policy_id