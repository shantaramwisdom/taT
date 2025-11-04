-- File: general_ledger_header_count.sql
with source_data as (
    select trim(activity_reported_dt) as transaction_date,
    substr(trim(app_area_source_cd),1,2) as gl_application_area_code,
    trim(source_sys_cd) as gl_source_code,
    trim(legal_entity) || trim(ledger) as secondary_ledger_code,
    'Financial' as data_type,
    trim(parent_trans_id) as sourceactivityparentid
    from {source_database}.cwtacotchub_interface_current
    where cycle_date = {cycledate}
),
source as (
    select count(*) as src_cnt
    from source_data
),
source_adj as (
    select count(*) as src_adj_cnt
    from source_data a
    left join lkp_actvty_gl_sblgdr_nm b on substr(gl_application_area_code,1,2) = b.ACTVTY_GL_APP_AREA_CD
    and gl_source_code = b.ACTVTY_GL_SRC_CD
    left join lkp_actvty_ldgr_nm c on substr(secondary_ledger_code,3,2) = c.actvty_gl_ldgr_cd
    where c.include_exclude = 'Exclude'
),
target_data as (
    select python_date_format_checker(transaction_date,'%d%m%Y') as transaction_date_chk,
    gl_source_code,
    secondary_ledger_code,
    transaction_date,
    gl_application_area_code,
    data_type,
    sourceactivityparentid,
    substr(gl_application_area_code,1,2) as gl_application_area_code_drvd,
    gl_source_code as gl_source_code_drvd,
    substr(secondary_ledger_code,3,2) as secondary_ledger_code_drvd,
    b.oracle_fah_ldgr_nm as ledger_name_drvd,
    b.src_sys_nm_desc as source_system_nm_drvd,
    b.evnt_typ_cd as event_type_code_drvd,
    b.oracle_fah_subldr_nm as subledger_short_name_drvd
    from source_data a
    left join lkp_actvty_gl_sblgdr_nm b on substr(gl_application_area_code,1,2) = b.ACTVTY_GL_APP_AREA_CD
    and gl_source_code = b.ACTVTY_GL_SRC_CD
    left join lkp_actvty_ldgr_nm c on substr(secondary_ledger_code,3,2) = c.actvty_gl_ldgr_cd
    where nvl(c.include_exclude,'NULLVAL') in ('Include','NULLVAL')
),
target_force_ignore as (
    select distinct gl_application_area_code_drvd,
    gl_source_code_drvd
    from target_data a
    left semi join (
        select gl_application_area_code_drvd,
        gl_source_code_drvd
        from target_data
        where transaction_date_chk = 'true'
        and ledger_name_drvd is not null
        and source_system_nm_drvd is not null
        and event_type_code_drvd is not null
        and subledger_short_name_drvd is not null
        and gl_source_code not like '%X%'
        and secondary_ledger_code not like '%X%'
        and len(gl_application_area_code) > 0
        and len(gl_source_code) > 0
        and len(secondary_ledger_code) > 0
        group by gl_application_area_code_drvd,
        gl_source_code_drvd
    ) b using (gl_application_area_code_drvd, gl_source_code_drvd)
    where transaction_date_chk = 'false'
),
target_ignore as (
    select count(*) as target_ign_count
    from (
        select transaction_date,
        gl_application_area_code_drvd,
        data_type,
        sourceactivityparentid,
        gl_source_code_drvd,
        secondary_ledger_code_drvd,
        ledger_name_drvd,
        source_system_nm_drvd,
        event_type_code_drvd,
        subledger_short_name_drvd
        from (
            select a.*,
            row_number() over (
                partition by transaction_date,
                gl_application_area_code_drvd,
                data_type,
                sourceactivityparentid,
                gl_source_code_drvd,
                secondary_ledger_code_drvd,
                ledger_name_drvd,
                source_system_nm_drvd,
                event_type_code_drvd,
                subledger_short_name_drvd
            ) as fltr
            from target_data a
            left anti join target_force_ignore b using (gl_application_area_code_drvd, gl_source_code_drvd)
            where transaction_date_chk = 'true'
            and ledger_name_drvd is not null
            and source_system_nm_drvd is not null
            and event_type_code_drvd is not null
            and subledger_short_name_drvd is not null
            and gl_source_code not like '%X%'
            and secondary_ledger_code not like '%X%'
            and len(gl_application_area_code) > 0
            and len(gl_source_code) > 0
            and len(secondary_ledger_code) > 0
        )
        where fltr > 1
    )
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
        or gl_application_area_code is null
        or gl_source_code like '%X%'
        or gl_source_code like 'X%'
        or secondary_ledger_code like '%X%'
        or len(gl_application_area_code) = 0
        or len(gl_source_code) = 0
        or len(secondary_ledger_code) = 0
        union all
        select count(*) as err_cnt
        from target_data a
        left semi join target_force_ignore b using (gl_application_area_code_drvd, gl_source_code_drvd)
        where transaction_date_chk = 'true'
        and ledger_name_drvd is not null
        and source_system_nm_drvd is not null
        and event_type_code_drvd is not null
        and subledger_short_name_drvd is not null
        and gl_source_code not like '%X%'
        and secondary_ledger_code not like '%X%'
        and len(gl_application_area_code) > 0
        and len(gl_source_code) > 0
        and len(secondary_ledger_code) > 0
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
    and (
        a.original_cycle_date != b.cycle_date
        or a.original_batch_id != b.batch_id
    )
    and b.batch_frequency = '{batch_frequency}'
    and b.cycle_date = '{cycle_date}'
    and b.batch_id = '{batchid}'
)
select {batchid} batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'BALANCING_COUNT' measure_name,
'curated' hop_name,
'recordcount' measure_field,
'INTEGER' measure_value_datatype,
'S' measure_src_tgt_adj_indicator,
'{source_database}.accounting_current' measure_table,
src_cnt measure_value
from source
union all
select {batchid} batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'BALANCING_COUNT' measure_name,
'curated' hop_name,
'recordcount' measure_field,
'INTEGER' measure_value_datatype,
'SI' measure_src_tgt_adj_indicator,
'{source_database}.accounting_current' measure_table,
err_cnt measure_value
from new_errors
union all
select {batchid} batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'BALANCING_COUNT' measure_name,
'curated' hop_name,
'recordcount' measure_field,
'INTEGER' measure_value_datatype,
'A' measure_src_tgt_adj_indicator,
'{source_database}.accounting_current' measure_table,
src_adj_cnt measure_value
from source_adj
union all
select {batchid} batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'BALANCING_COUNT' measure_name,
'curated' hop_name,
'recordcount' measure_field,
'INTEGER' measure_value_datatype,
'TA' measure_src_tgt_adj_indicator,
'{source_database}.accounting_current' measure_table,
clr_cnt measure_value
from errors_cleared
union all
select {batchid} batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'BALANCING_COUNT' measure_name,
'curated' hop_name,
'recordcount' measure_field,
'INTEGER' measure_value_datatype,
'TI' measure_src_tgt_adj_indicator,
'{source_database}.accounting_current' measure_table,
target_ign_count measure_value
from target_ignore;