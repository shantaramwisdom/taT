with source_data as (
 select trim(CIREC802_POL_FILE_DATE) as transaction_date,
        trim(cirec802_pol_le) || trim(cirec802_pol_lg) as secondary_ledger_code,
        'Financial' as data_type,
        'TAI ASIA' as source_system_nm,
        trim(CIREC802_POL_REF_NUMBER) as contractnumber,
        CAST('{cycle_date}' AS DATE) as cycle_date
 from {source_database}.accounting_detail_current
 where cycle_date = {cycledate}
),
source as (
 select count(*) as src_cnt
 from source_data
),
source_adj as (
 select count(*) as src_adj_cnt
 from source_data a
 left join lkp_actvty_gl_sbldgr_nm b on '~' = b.ACTVTY_GL_APP_AREA_CD
                                      and '~' = b.ACTVTY_GL_SRC_CD
                                      and 'TAI ASIA' = b.src_sys_nm_desc
 left join lkp_actvty_ldgr_nm c on substr(secondary_ledger_code, 3, 2) = c.actvty_gl_ldgr_cd
                                and secondary_ledger_code = c.sec_ldgr_cd
 where c.include_exclude = 'Exclude'
),
target_data as (
 select python_date_format_checker(transaction_date, '%m/%d/%Y') as transaction_date_chk,
        secondary_ledger_code,
        transaction_date,
        data_type,
        contractnumber,
        cycle_date,
        substr(secondary_ledger_code, 3, 2) as secondary_ledger_code_drvd,
        b.oracle_fah_ldgr_nm as ledger_name_drvd,
        source_system_nm as source_system_nm_drvd,
        b.evnt_typ_cd as event_type_code_drvd,
        b.oracle_fah_sbldgr_nm as subledger_short_name_drvd,
        monotonically_increasing_id() random_counter
 from source_data a
 left join lkp_actvty_gl_sbldgr_nm b on '~' = b.ACTVTY_GL_APP_AREA_CD
                                      and '~' = b.ACTVTY_GL_SRC_CD
                                      and 'TAI ASIA' = b.src_sys_nm_desc
 left join lkp_actvty_ldgr_nm c on substr(secondary_ledger_code, 3, 2) = c.actvty_gl_ldgr_cd
                                and secondary_ledger_code = c.sec_ldgr_cd
 where nvl(c.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
),
target_force_ignore as (
 select distinct cycle_date
 from target_data
 where transaction_date_chk = 'false'
),
new_errors as (
 select sum(err_cnt) as err_cnt
 from (
       select count(*) as err_cnt
       from target_data a
       where transaction_date_chk = 'false'
         or ledger_name_drvd is null
         or source_system_nm_drvd is null
         or event_type_code_drvd is null
         or subledger_short_name_drvd is null
         or secondary_ledger_code like '% %'
         or len(secondary_ledger_code) < 2
       union all
       select count(*) as err_cnt
       from target_data a
       left semi join target_force_ignore b using (cycle_date)
       where transaction_date_chk = 'true'
         and ledger_name_drvd is not null
         and source_system_nm_drvd is not null
         and event_type_code_drvd is not null
         and subledger_short_name_drvd is not null
         and secondary_ledger_code not like '% %'
         and len(secondary_ledger_code) >= 4
 )
),
errors_cleared as (
 select count(*) clr_cnt
 from {curated_database}.currentbatch b
 join {curated_database}.{curated_table_name} a using (cycle_date, batch_id)
 where b.source_system = '{source_system_name}'
   and b.domain_name = '{domain_name}'
   and a.original_cycle_date is not null
   and a.original_batch_id is not null
   and (a.original_cycle_date != b.cycle_date
        or a.original_batch_id != b.batch_id)
   and b.batch_frequency = '{batch_frequency}'
   and b.cycle_date = '{cycle_date}'
   and b.batch_id = '{batchid}'
)
SELECT {batchid} batch_id,
       '{cycle_date}' cycle_date,
       '{domain_name}' domain,
       '{source_system_name}' sys_nm,
       '{source_system_name}' p_sys_nm,
       'BALANCING_COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'INTEGER' measure_value_datatype,
       'S' measure_src_tgt_adj_indicator,
       '{source_database}.accounting_detail_current' measure_table,
       src_cnt measure_value
FROM source
UNION ALL
SELECT {batchid} batch_id,
       '{cycle_date}' cycle_date,
       '{domain_name}' domain,
       '{source_system_name}' sys_nm,
       '{source_system_name}' p_sys_nm,
       'BALANCING_COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'INTEGER' measure_value_datatype,
       'SI' measure_src_tgt_adj_indicator,
       '{source_database}.accounting_detail_current' measure_table,
       err_cnt measure_value
FROM new_errors
UNION ALL
SELECT {batchid} batch_id,
       '{cycle_date}' cycle_date,
       '{domain_name}' domain,
       '{source_system_name}' sys_nm,
       '{source_system_name}' p_sys_nm,
       'BALANCING_COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'INTEGER' measure_value_datatype,
       'A' measure_src_tgt_adj_indicator,
       '{source_database}.accounting_detail_current' measure_table,
       src_adj_cnt measure_value
FROM source_adj
UNION ALL
SELECT {batchid} batch_id,
       '{cycle_date}' cycle_date,
       '{domain_name}' domain,
       '{source_system_name}' sys_nm,
       '{source_system_name}' p_sys_nm,
       'BALANCING_COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'INTEGER' measure_value_datatype,
       'TA' measure_src_tgt_adj_indicator,
       '{source_database}.accounting_detail_current' measure_table,
       clr_cnt measure_value
FROM errors_cleared;
