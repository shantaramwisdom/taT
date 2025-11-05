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
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
source_adj_data as (
    select a.*
    from source_data a
             left join lkp_actvty_gl_sbldgr_nm b on a.gl_application_area_code = b.ACTVTY_GL_APP_AREA_CD
             and a.gl_source_code = b.ACTVTY_GL_SRC_CD
             left join lkp_lgl_enty_cd d on d.cntrt_src_sys_nm = 'Illumifin' and d.src_lgl_enty_cd = a.sourcelegalentitycode
             left join lkp_actvty_ldgr_nm c on c.actvty_gl_ldgr_cd = a.secondary_ledger_code
             and d.lgl_enty_cd = c.actvty_lgl_enty_cd
    where c.include_exclude = 'Exclude'
       or a.orig_gl_company not in ('301', '501', '701')
),
target_data as (
    select python_date_format_checker(transaction_date, '%Md%Y') as transaction_date_chk,
           gl_source_code,
           secondary_ledger_code,
           transaction_date,
           gl_application_area_code,
           data_type,
           sourcelegalentitycode,
           sourcesystemgeneralledgeraccountnumber,
           orig_gl_company,
           orig_gl_account,
           orig_gl_center,
           d.lgl_enty_cd||'01' as orig_gl_company_drvd,
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
        group by gl_application_area_code,
                 gl_source_code_drvd
    ) b using (gl_application_area_code, gl_source_code_drvd)
    where transaction_date_chk = 'false'
),
target_ignore as (
    select gl_application_area_code,
           transaction_date
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
    ) as a
    where fltr > 1
),
source_ignore as (
    select gl_application_area_code,
           transaction_date
    from (
        select a.*,
               row_number() over (
                   partition by gl_source_code,
                                secondary_ledger_code,
                                transaction_date,
                                gl_application_area_code,
                                data_type,
                                sourcelegalentitycode,
                                sourcesystemgeneralledgeraccountnumber,
                                orig_gl_company,
                                orig_gl_account,
                                orig_gl_center,
                                gl_source_code_drvd,
                                orig_gl_company_drvd,
                                orig_gl_center_drvd,
                                orig_gl_account_drvd,
                                secondary_ledger_code_drvd,
                                ledger_name_drvd,
                                source_system_nm_drvd,
                                event_type_code_drvd,
                                subledger_short_name_drvd
                   order by null
               ) as fltr
        from (
            select a.*
            from target_data a
            where transaction_date_chk = 'false'
               or ledger_name_drvd is null
               or source_system_nm_drvd is null
               or event_type_code_drvd is null
               or orig_gl_center_drvd is null
               or orig_gl_account_drvd is null
               or orig_gl_company_drvd is null
               or subledger_short_name_drvd is null
            union all
            select a.*
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
              and cast(nvl(transaction_date, 0) as INTEGER) > 0
        ) as a
    ) as a
    where fltr > 1
)
select cast('{cycle_date}' as date) as cycle_date,
       cast({batchid} as int) batch_id,
       gl_application_area_code,
       cast(substr(transaction_date, -4) as numeric) as transaction_date,
       measure_type_code
from (
    select gl_application_area_code,
           transaction_date,
           'S' as measure_type_code
    from source_data
    union all
    select gl_application_area_code,
           transaction_date,
           'SA' as measure_type_code
    from source_adj_data
    union all
    select gl_application_area_code,
           transaction_date,
           'TI' as measure_type_code
    from target_ignore
    union all
    select gl_application_area_code,
           transaction_date,
           'SI' as measure_type_code
    from source_ignore
) a;

