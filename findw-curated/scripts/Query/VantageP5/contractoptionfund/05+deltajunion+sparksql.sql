select 
inf.ex_policy_number as key_qualifier,
'PS' as system_name,
inf.batchid,
db.company_code
from 
(
    select 
    ex_policy_number,
    batchid
    from 
    {source_database}.gdqp5inforceevaluation
    where 
    cast(batchid as int) = cast('{batchid}' as int)
) inf
left join gdqp5dprodsegmentdb db on 
db.key_qualifier = inf.ex_policy_number
union
select 
key_qualifier,
system_name,
batchid,
company_code
from 
gdqp5deltaextract_mainfile_keys delta
where 
cast(delta.batchid as int) = {batchid}
and delta.system_name = 'PS'
and (delta.file_name like 'CLPROXX'
and delta.segment_id in ('DB','EB'))
