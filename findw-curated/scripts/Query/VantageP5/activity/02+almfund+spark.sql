SELECT generatesameuuid(concat(fundnumber,':',fundregion)) as documentid,
coalesce(fundnumber_override, c_fundnumber, fundnumber) as fundnumber,
coalesce(fundregion_override, c_fundregion, fundregion) as fundregion
FROM {source_database}.gdqalfundmapping
WHERE cast(batchid as int) = (select max(cast(batchid as int))
from {source_database}.gdqcompletedbatchidinfo
where source_system = 'aim'
and cast(rtaa_effective_date as date) <= cast('{cycle_date}' as date));
