select {batchid} batch_id,
       '{cycle_date}' cycle_date,
       '{domain_name}' domain,
       '{source_system_name}' sys_nm,
       '{source_system_name}' p_sys_nm,
       'BALANCING_COUNT' measure_name, 'curated' hop_name,
       'recordcount' measure_field,
       'INTEGER' measure_value_datatype,
       'S' measure_src_tgt_adj_indicator,
       edgalfundmapping measure_table,
       count(*) measure_value
from (select distinct fundnumber, fundregion
      from {source_database}.edgalfundmapping
      where batchid={batchid})