select {batchid} batch_id, '{cycle_date}' cycle_date, '{domain_name}' domain, '{source_system_name}' sys_nm, '{source_system_name}' p_sys_nm
,'BALANCING COUNT' measure_name, 'Curated' hop_name
,'recordcount' measure_field, 'INTEGER' measure_value_datatype, 'S' measure_src_tgt_adj_indicator
,'gdqp5trxhist / gdqp5trxhist_fund_info / gdqp5suspense' measure_table
,count(*) measure_value
from (
select key_qualifier from {source_database}.gdqp5trxhist where batchid={batchid}
union all
select key_qualifier from {source_database}.gdqp5trxhist_fund_info where batchid={batchid}
union all
select key_qualifier from {source_database}.gdqp5suspense where batchid={batchid}
)a
