


select
life.life_fname,
life.life_lname,
life.life_mname,
life.international_address_flg,
life.life_address1,
life.intl_line_1,
life.life_address2,
life.intl_line_2,
life.life_city,
life.intl_line_3,
life.INTL_LINE_4,
life.INTL_LINE_5,
life.INTL_LINE_7,
life.life_id,
life.life_zip,
life.intl_line_6,
life.life_state,
life.cycle_date,
life.intl_style_cd,
policy.policy_no as contractnumber,
'{cycle_date}' as cycle_date,
'{batchid}' as batchid
from
{source_database}.life_effective_history_daily_{cycledate} life
left join
{source_database}.Policy_effective_history_daily_{cycledate} policy
on life.life_id = policy.insured_life_id
where
cast(life.view_row_create_day as date)<= '{cycle_date}'
and '{cycle_date}' <= cast(life.view_row_obsolete_day as date)
and cast(life.view_row_effective_day as date)<= '{cycle_date}'
and '{cycle_date}' <= cast(life.view_row_expiration_day as date)
