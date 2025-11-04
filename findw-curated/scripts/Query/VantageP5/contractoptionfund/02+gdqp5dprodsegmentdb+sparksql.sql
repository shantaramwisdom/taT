select 
db.*,
case 
    when delta.operation_code = 'DELT' then 'DELT' 
    else 'NON-DELT' 
end as operation_code
from 
(
    select 
    coalesce(key_qualifier_override, c_key_qualifier, key_qualifier) as contractnumber,
    coalesce(company_code_override, c_company_code, company_code) as contractadministrationlocationcode,
    key_qualifier,
    company_code,
    ordinal_position,
    segment_id,
    batchid,
    segment_sequence_number,
    row_number() over(partition by key_qualifier,
                                    company_code,
                                    segment_id,
                                    ordinal_position
                      order by 
                          cast(batchid as int) desc,
                          segment_sequence_number desc) as seq_num
    from 
    {source_database}.gdqp5dprodsegmentdb
    where 
    cast(batchid as int) <= {batchid}
) db
where 
seq_num = 1
left join gdqp5deltaextract_mainfile_keys delta on 
db.key_qualifier = delta.key_qualifier
and db.company_code = delta.company_code
and delta.segment_id = 'DB'
