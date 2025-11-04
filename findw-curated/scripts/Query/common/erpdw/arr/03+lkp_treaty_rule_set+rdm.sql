select feedtype,
reinsurance_treaty_component_identifier,
geac_account,
activitybalanceindicator,
transactiontype,
treatyruleset,
multiplier,
treaty_number,
include_exclude
from time.sch.lkp_treaty_rule_set
where {other_disable}
to_date('{cycledate}','yyyymmdd') between eff_strt_dt and eff_stop_dt
and feedtype = 'ARR Feed'
