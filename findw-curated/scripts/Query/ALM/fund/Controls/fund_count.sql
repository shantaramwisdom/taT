select {batchid} batch_id
,'{cycle_date}' cycle_date
,'{domain_name}' domain_in
,'{source_system_name}' sys_nm
,'{source_system_name}' p_sys_nm
,'BALANCING_COUNT' measure_name, 'curated' hop_name
,'recordcount' measure_field
,'INTEGER' measure_value_datatype
,'S' measure_src_tgt_adj_indicator
,'gdqalmfundmapping' measure_table
,count(*) measure_value
FROM
(select distinct fundnumber, fundregion
from {source_database}.gdqalmfundmapping
where batchid={batchid})
