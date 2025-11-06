with source_data as (
    select trim(journ_date) as transaction_date,
           'WH' as gl_application_area_code,
           '499' as gl_source_code,
           '01' as secondary_ledger_code,
           'Financial' as data_type,
           trim(policy) as contractnumber,
           nvl(
                   cast(
                           cast(
                                   nvl(trim(amount), 0) as numeric(18, 2)
                               ) as decimal(18, 2)
                       ),
                   0
               ) as default_amount,
           trim(source_company) as orig_gl_company,
           trim(product_fund_code) as orig_gl_account,
           trim(product_fund_code) as orig_gl_center,
           trim(account) as sourcesystemgeneralledgeraccountnumber
    from {source_database}.accounting_detail_current
    where cycle_date = {cycledate}
),

source as (
    select sum(default_amount) as src_default_amount
    from source_data
),

source_adj as (
    select sum(default_amount) as src_adj_default_amount
    from source_data a
             left join lkp_actvty_gl_sblgdr_nm b on a.gl_application_area_code = b.ACTVTY_GL_APP_AREA_CD
        and a.gl_source_code = b.ACTVTY_GL_SRC_CD
             left join lkp_lgl_enty_cd c2 on c2.cntrct_src_sys_nm = 'Illumifin' and c2.src_lgl_enty_cd = a.orig_gl_company
             left join lkp_actvty_ldgr_nm c on '01' = c.actvty_gl_ldgr_cd
        and c2.lgl_enty_cd = c.actvty_lgl_enty_cd
    where c.include_exclude = 'Exclude'
       or a.orig_gl_company not in ('301', '501', '701')
),

target_data as (
    select python_date_format_checker(transaction_date, '%m%d%Y') as transaction_date_chk,
           gl_source_code,
           secondary_ledger_code,
           transaction_date,
           gl_application_area_code,
           data_type,
           contractnumber,
           c2.lgl_enty_cd||'01' as orig_gl_company_drvd,
           case
               when orig_gl_center is null then i.cntr_nmbr
               else j.cntr_nmbr
               end as orig_gl_center_drvd,
           case
               when orig_gl_account is null then i.gnrl_ldgr_acct_nmbr || i.gnrl_ldgr_acct_sffx
               else j.gnrl_ldgr_acct_nmbr || j.gnrl_ldgr_acct_sffx
               end as orig_gl_account_drvd,
           gl_source_code as gl_source_code_drvd,
           secondary_ledger_code as secondary_ledger_code_drvd,
           c.oracle_fah_ldgr_nm as ledger_name_drvd,
           b.src_sys_nm_desc as source_system_nm_drvd,
           b.evnt_typ_cd as event_type_code_drvd,
           b.oracle_fah_sublgdr_nm as subledger_short_name_drvd,
           default_amount,
           monotonically_increasing_id() random_counter
    from source_data a
             left join lkp_actvty_gl_sblgdr_nm b on a.gl_application_area_code = b.ACTVTY_GL_APP_AREA_CD
        and a.gl_source_code = b.ACTVTY_GL_SRC_CD
             left join lkp_lgl_enty_cd c2 on c2.cntrct_src_sys_nm = 'Illumifin' and c2.src_lgl_enty_cd = a.orig_gl_company
             left join lkp_actvty_ldgr_nm c on '01' = c.actvty_gl_ldgr_cd
        and c2.lgl_enty_cd = c.actvty_lgl_enty_cd
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
        group by gl_application_area_code,
                 gl_source_code_drvd
    ) b using (gl_application_area_code, gl_source_code_drvd)
    where transaction_date_chk = 'false'
),

new_errors as (
    select sum(err_default_amount) as err_default_amount
    from (
             select sum(default_amount) as err_default_amount
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
             select sum(default_amount) as err_default_amount
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
    select sum(default_amount) as clr_default_amount
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
       'CONTROL_TOTAL' measure_name,
       'curated' hop_name,
       'fsl_gl_amount' measure_field,
       'INTEGER' measure_value_datatype,
       'S' measure_src_tgt_adj_indicator,
       '{source_database}.accounting_detail' measure_table,
       nvl(src_default_amount, 0) as measure_value
FROM source
UNION ALL
SELECT {batchid} batch_id,
       '{cycle_date}' cycle_date,
       '{domain_name}' domain,
       '{source_system_name}' sys_nm,
       '{source_system_name}' p_sys_nm,
       'CONTROL_TOTAL' measure_name,
       'curated' hop_name,
       'fsi_gl_pt_amount' measure_field,
       'INTEGER' measure_value_datatype,
       'SI' measure_src_tgt_adj_indicator,
       '{source_database}.accounting_detail' measure_table,
       nvl(err_default_amount, 0) as measure_value
FROM new_errors
UNION ALL
SELECT {batchid} batch_id,
       '{cycle_date}' cycle_date,
       '{domain_name}' domain,
       '{source_system_name}' sys_nm,
       '{source_system_name}' p_sys_nm,
       'CONTROL_TOTAL' measure_name,
       'curated' hop_name,
       'fsi_gl_pt_amount' measure_field,
       'INTEGER' measure_value_datatype,
       'A' measure_src_tgt_adj_indicator,
       '{source_database}.accounting_detail' measure_table,
       nvl(src_adj_default_amount, 0) as measure_value
FROM source_adj
UNION ALL
SELECT {batchid} batch_id,
       '{cycle_date}' cycle_date,
       '{domain_name}' domain,
       '{source_system_name}' sys_nm,
       '{source_system_name}' p_sys_nm,
       'CONTROL_TOTAL' measure_name,
       'curated' hop_name,
       'fsi_gl_pt_amount' measure_field,
       'INTEGER' measure_value_datatype,
       'TA' measure_src_tgt_adj_indicator,
       '{source_database}.accounting_detail' measure_table,
       nvl(clr_default_amount, 0) as measure_value
FROM errors_cleared

