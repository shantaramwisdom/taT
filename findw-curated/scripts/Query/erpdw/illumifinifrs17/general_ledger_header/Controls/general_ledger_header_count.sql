with source_data as (
    select trim(journ_date) as transaction_date,
           'WM' as gl_application_area_code,
           '499' as gl_source_code,
           '01' as secondary_ledger_code,
           'Financial' as data_type,
           trim(source_company) as sourcelegalentitycode,
           trim(account) as sourcesystemgeneralledgeraccountnumber,
           trim(source_company) as orig_gl_company,
           trim(product_fund_code) as orig_gl_account,
           trim(product_fund_code) as orig_gl_center
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
         left join lkp_actvty_gl_sbldgr_nm b on a.gl_application_area_code = b.ACTVTY_GL_APP_AREA_CD
         and a.gl_source_code = b.ACTVTY_GL_SRC_CD
         left join lkp_lgl_enty_cd d on d.cntrt_src_sys_nm = 'Illumifin' and d.src_lgl_enty_cd = a.sourcelegalentitycode
         left join lkp_actvty_ldgr_nm c on c.actvty_gl_ldgr_cd = a.secondary_ledger_code
         and d.lgl_enty_cd = c.actvty_lgl_enty_cd
         left join lkp_src_acct_to_gl_acct i on i.src_lgl_enty = a.orig_gl_company
         and i.src_sys_acct_nmbr = a.sourcesystemgeneralledgeraccountnumber
         and i.src_sys_prdct_cd = a.orig_gl_center
         left join lkp_src_acct_to_gl_acct j on j.src_lgl_enty = a.orig_gl_company
         and j.src_sys_acct_nmbr = a.sourcesystemgeneralledgeraccountnumber
         and j.src_sys_prdct_cd = a.orig_gl_center
    where nvl(c.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
      or a.orig_gl_company not in ('301', '501', '701')
),
target_data as (
    select python_date_format_checker(transaction_date, '%m%d%Y') as transaction_date_chk,
           gl_source_code,
           secondary_ledger_code,
           transaction_date,
           gl_application_area_code,
           data_type,
           sourcelegalentitycode,
           d.lgl_enty_cd||'01' as orig_gl_company_drvd,
           case
               when orig_gl_center is null then i.cntr_nmbr
               else i.cntr_nmbr
           end as orig_gl_center_drvd,
           case
               when orig_gl_account is null then i.gnrl_ldgr_acct_nmbr || i.gnrl_ldgr_acct_sffx
               else i.gnrl_ldgr_acct_nmbr || i.gnrl_ldgr_acct_sffx
           end as orig_gl_account_drvd,
           gl_source_code as gl_source_code_drvd,
           secondary_ledger_code as secondary_ledger_code_drvd,
           c.oracle_fah_ldgr_nm as ledger_name_drvd,
           b.src_sys_nm_desc as source_system_nm_drvd,
           b.evnt_typ_cd as event_type_code_drvd,
           b.oracle_fah_sbldgr_nm as subledger_short_name_drvd
    from source_data a
         left join lkp_actvty_gl_sbldgr_nm b on a.gl_application_area_code = b.ACTVTY_GL_APP_AREA_CD
         and a.gl_source_code = b.ACTVTY_GL_SRC_CD
         left join lkp_lgl_enty_cd d on d.cntrt_src_sys_nm = 'Illumifin' and d.src_lgl_enty_cd = a.sourcelegalentitycode
         left join lkp_actvty_ldgr_nm c on c.actvty_gl_ldgr_cd = a.secondary_ledger_code
         and d.lgl_enty_cd = c.actvty_lgl_enty_cd
         left join lkp_src_acct_to_gl_acct i on i.src_lgl_enty = a.orig_gl_company
         and i.src_sys_acct_nmbr = a.sourcesystemgeneralledgeraccountnumber
         and i.src_sys_prdct_cd = a.orig_gl_center
         left join lkp_src_acct_to_gl_acct j on j.src_lgl_enty = a.orig_gl_company
         and j.src_sys_acct_nmbr = a.sourcesystemgeneralledgeraccountnumber
         and j.src_sys_prdct_cd = a.orig_gl_center
    where nvl(c.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
      and a.orig_gl_company in ('301', '501', '701')
),
target_force_ignore as (
    select distinct gl_application_area_code,
                    gl_source_code_drvd
    from target_data a
         left semi join (
             select gl_application_area_code,
                    gl_source_code_drvd
             from target_data
             where transaction_date_chk = 'true'
               and ledger_name_drvd is not null
               and source_system_nm_drvd is not null
               and event_type_code_drvd is not null
               and orig_gl_center_drvd is not null
               and orig_gl_account_drvd is not null
               and orig_gl_company_drvd is not null
               and subledger_short_name_drvd is not null
               and cast(nvl(transaction_date, 0) as INTEGER) > 0
             group by gl_application_area_code, gl_source_code_drvd
         ) b using (gl_application_area_code, gl_source_code_drvd)
    where transaction_date_chk = 'false'
),
target_ignore as (
    select count(*) as target_ign_count
    from (
        select transaction_date,
               gl_application_area_code,
               data_type,
               sourcelegalentitycode,
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
                                    gl_application_area_code,
                                    data_type,
                                    sourcelegalentitycode,
                                    gl_source_code_drvd,
                                    secondary_ledger_code_drvd,
                                    ledger_name_drvd,
                                    source_system_nm_drvd,
                                    event_type_code_drvd,
                                    subledger_short_name_drvd
                       order by null
                   ) as fltr
            from target_data a
                     left anti join target_force_ignore b using (gl_application_area_code, gl_source_code_drvd)
            where transaction_date_chk = 'true'
              and ledger_name_drvd is not null
              and source_system_nm_drvd is not null
              and event_type_code_drvd is not null
              and orig_gl_center_drvd is not null
              and orig_gl_account_drvd is not null
              and orig_gl_company_drvd is not null
              and subledger_short_name_drvd is not null
        ) where fltr > 1
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
           or orig_gl_center_drvd is null
           or orig_gl_account_drvd is null
           or orig_gl_company_drvd is null
           or subledger_short_name_drvd is null
           or gl_application_area_code is null
        union all
        select count(*) as err_cnt
        from target_data a
                 left semi join target_force_ignore b using (gl_application_area_code, gl_source_code_drvd)
        where transaction_date_chk = 'true'
          and ledger_name_drvd is not null
          and source_system_nm_drvd is not null
          and event_type_code_drvd is not null
          and orig_gl_center_drvd is not null
          and orig_gl_account_drvd is not null
          and orig_gl_company_drvd is not null
          and subledger_short_name_drvd is not null
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
      and b.batch_id = {batchid}
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
       '{source_database}.accounting_detail' measure_table,
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
       'ST' measure_src_tgt_adj_indicator,
       '{source_database}.accounting_detail' measure_table,
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
       '{source_database}.accounting_detail' measure_table,
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
       '{source_database}.accounting_detail' measure_table,
       clr_cnt measure_value
FROM errors_cleared
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
       'TI' measure_src_tgt_adj_indicator,
       '{source_database}.accounting_detail' measure_table,
       target_ign_count measure_value
FROM target_ignore;

