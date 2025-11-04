select 
key_qualifier,
company_code,
segment_id,
system_name,
segment_sequence_number,
file_name,
batchid,
case 
    when operation_code = 'DELT' then 'DELT' 
    else 'NON-DELT' 
end as operation_code
from 
(
    select 
    key_qualifier,
    company_code,
    segment_id,
    system_name,
    segment_sequence_number,
    file_name,
    batchid,
    operation_code,
    row_number() over(partition by key_qualifier,
                                    company_code,
                                    segment_id,
                                    case 
                                        when segment_id in ('EB','EBU') then segment_sequence_number 
                                        else null 
                                    end
                     order by 
                         cast(batchid as int) desc,
                         case 
                             when operation_code = 'DELT' then 2 
                             else 1 
                         end) as seq_num
    from 
    {source_database}.gdqp5deltaextract_mainfile_keys
    where 
    cast(batchid as int) <= {batchid}
    and system_name = 'PS'
    and file_name like 'CLPROXX'
    and segment_id in ('DB','EB','EBU')
) del
where 
del.seq_num = 1
