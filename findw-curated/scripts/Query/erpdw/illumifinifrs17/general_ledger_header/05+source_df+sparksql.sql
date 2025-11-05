with input as (
    select *,
           from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as original_recorded_timestamp,
           CAST('{cycle_date}' AS DATE) as original_cycle_date,
           CAST({batchid} AS INT) as original_batch_id,
           cast(null as string) as old_error_message,
           cast(null as int) as error_record_aging_days
    from (
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
    )
    union all
    select transaction_date,
           gl_application_area_code,
           gl_source_code,
           secondary_ledger_code,
           data_type,
           sourcelegalentitycode,
           sourcesystemgeneralledgeraccountnumber,
           orig_gl_company,
           orig_gl_account,
           orig_gl_center,
           original_recorded_timestamp,
           original_cycle_date,
           original_batch_id,
           error_message,
           error_record_aging_days
    from error_header
),
master as (
    select transaction_date,
           gl_application_area_code,
           gl_source_code,
           secondary_ledger_code,
           data_type,
           sourcelegalentitycode,
           sourcesystemgeneralledgeraccountnumber,
           orig_gl_company,
           orig_gl_account,
           orig_gl_center,
           gl_source_code as gl_source_code_drvd,
           secondary_ledger_code as secondary_ledger_code_drvd,
           c.oracle_fah_ldgr_nm as ledger_name_drvd,
           b.src_sys_nm_desc as source_system_nm_drvd,
           b.evnt_typ_cd as event_type_code_drvd,
           b.oracle_fah_sbldgr_nm as subledger_short_name_drvd,
           python_date_format_checker(transaction_date, 'MMddyy') as transaction_date_chk,
           case
               when transaction_date_chk = 'true' then to_date(transaction_date, 'MMddyyyy')
               else null
           end as transaction_date_drvd,
           d.lgl_enty_cd || '01' as orig_gl_company_drvd,
           case
               when orig_gl_center is null then i.cntr_nmbr
               else i.cntr_nmbr
           end as orig_gl_center_drvd,
           case
               when orig_gl_account is null then i.gnrl_ldgr_acct_nmbr || i.gnrl_ldgr_acct_sffx
               else i.gnrl_ldgr_acct_nmbr || i.gnrl_ldgr_acct_sffx
           end as orig_gl_account_drvd,
           cast(original_recorded_timestamp as timestamp) as original_recorded_timestamp,
           cast(original_cycle_date as date) as original_cycle_date,
           cast(original_batch_id as int) as original_batch_id,
           b.include_exclude as sbldgr_include_exclude,
           c.include_exclude as ldgr_include_exclude,
           old_error_message,
           error_record_aging_days
    from input a
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
keys as (
    select *,
           substr(event_type_code_drvd, 1, 6) || date_format(
               from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central'),
               'yyyyMMddHHmmssSSS'
           ) || '_' || (row_number() over (
               partition by subledger_short_name_drvd
               order by null
           ), 10, '0') as transaction_number_drvd
    from (
        select distinct ledger_name_drvd,
                        source_system_nm_drvd,
                        event_type_code_drvd,
                        subledger_short_name_drvd,
                        sourcelegalentitycode,
                        gl_source_code_drvd,
                        gl_application_area_code,
                        transaction_date_drvd,
                        data_type,
                        secondary_ledger_code_drvd,
                        original_cycle_date,
                        original_batch_id
        from master
        where ledger_name_drvd is not null
          and event_type_code_drvd is not null
          and source_system_nm_drvd is not null
          and subledger_short_name_drvd is not null
          and transaction_date_chk = 'true'
    )
),
final as (
    select a.*,
           b.transaction_number_drvd,
           trim(
               case
                   when transaction_date_chk = 'false' then nvl(transaction_date, '') || ' transaction_date is in invalid format, expected MMDDYYYY;'
                   else ''
               end
           ) as date_error_message_,
           trim(
               nvl(date_error_message_, '')
           ) as hard_error_message_,
           trim(
               case
                   when len(hard_error_message_) = 0 then case
                       when a.source_system_nm_drvd is null
                            or a.event_type_code_drvd is null then 'source_system_nm/event_type_code is missing in RDM table lkp_actvty_gl_sbldgr_nm for key combination gl_application_area_code (' || nvl(a.gl_application_area_code, '') || ') and gl_source_code (' || nvl(a.gl_source_code, '') || ');'
                       else ''
                   end
                   || case
                       when a.ledger_name_drvd is null then 'ledger_name is missing in RDM table lkp_actvty_ldgr_nm for key combination actvty_gl_ldgr_cd/secondary_ledger_code (' || nvl(a.secondary_ledger_code, '') || ') and actvty_lgl_enty_cd/ledger_name (' || nvl(a.sourcelegalentitycode, '') || ');'
                       else ''
                   end
                   || case
                       when a.subledger_short_name_drvd is null then 'subledger_short_name is missing in RDM table lkp_actvty_gl_sbldgr_nm for key combination gl_application_area_code (' || nvl(a.gl_application_area_code, '') || ') and gl_source_code (' || nvl(a.gl_source_code, '') || ');'
                       else ''
                   end
                   || case
                       when b.transaction_number_drvd is null then 'transaction_number is invalid;'
                       else ''
                   end
                   || case
                       when a.orig_gl_center_drvd is null then 'orig_gl_center is missing in RDM lkp_src_acct_to_gl_acct for orig_gl_center (' || nvl(a.orig_gl_center, '') || ');'
                       else ''
                   end
                   || case
                       when a.orig_gl_company_drvd is null then 'orig_gl_company is missing in RDM lkp_lgl_enty_cd for orig_gl_company (' || nvl(a.orig_gl_company, '') || ');'
                       else ''
                   end
                   || case
                       when a.orig_gl_account_drvd is null then 'orig_gl_account is missing in RDM lkp_src_acct_to_gl_acct for orig_gl_account (' || nvl(a.orig_gl_account, '') || ');'
                       else ''
                   end
               end
           ) as soft_error_message_,
           case
               when len(hard_error_message_) > 0 then 'N'
               when len(soft_error_message_) > 0 then 'Y'
               else null
           end as reprocess_flag,
           trim(
               concat(
                   nvl(hard_error_message_, ''),
                   nvl(soft_error_message_, '')
               )
           ) as error_message_,
           sbldgr_include_exclude,
           ldgr_include_exclude
    from master a
    left join keys b on a.source_system_nm_drvd = b.source_system_nm_drvd
                   and a.event_type_code_drvd = b.event_type_code_drvd
                   and a.ledger_name_drvd = b.ledger_name_drvd
                   and a.subledger_short_name_drvd = b.subledger_short_name_drvd
                   and a.transaction_date_drvd = b.transaction_date_drvd
                   and a.gl_application_area_code = b.gl_application_area_code
                   and a.gl_source_code_drvd = b.gl_source_code_drvd
                   and a.secondary_ledger_code_drvd = b.secondary_ledger_code_drvd
                   and a.data_type = b.data_type
                   and a.sourcelegalentitycode = b.sourcelegalentitycode
                   and a.original_cycle_date = b.original_cycle_date
                   and a.original_batch_id = b.original_batch_id
),
force_error_valid as (
    select distinct original_cycle_date,
                    original_batch_id,
                    gl_application_area_code,
                    gl_source_code_drvd
    from final
    where (gl_application_area_code, gl_source_code_drvd) in (
        select gl_application_area_code,
               gl_source_code_drvd
        from final
        where len(date_error_message_) > 0
          and original_cycle_date = '{cycle_date}'
          and original_batch_id = {batchid}
        group by gl_application_area_code,
                 gl_source_code_drvd
    )
      and len(error_message_) = 0
      and original_cycle_date = '{cycle_date}'
      and original_batch_id = {batchid}
)
select transaction_date,
       gl_application_area_code,
       gl_source_code,
       secondary_ledger_code,
       data_type,
       sourcelegalentitycode,
       sourcesystemgeneralledgeraccountnumber,
       orig_gl_company,
       orig_gl_account,
       orig_gl_center,
       orig_gl_company_drvd,
       orig_gl_account_drvd,
       orig_gl_center_drvd,
       gl_source_code_drvd,
       secondary_ledger_code_drvd,
       ledger_name_drvd,
       source_system_nm_drvd,
       event_type_code_drvd,
       subledger_short_name_drvd,
       transaction_date_chk,
       a.transaction_date_drvd,
       original_recorded_timestamp,
       a.original_cycle_date,
       a.original_batch_id,
       old_error_message,
       error_record_aging_days,
       a.transaction_number_drvd,
       trim(
           case
               when b.original_cycle_date is not null
                    and len(hard_error_message_) = 0 then 'Forced Error. Date Errors Present in Other Headers for combination of gl_application_area_code (' || nvl(a.gl_application_area_code, '') || ') and gl_source_code (' || nvl(a.gl_source_code_drvd, '') || ');'
               else ''
                 end || nvl(hard_error_message_, '')
   ) as hard_error_message,
   soft_error_message_ as soft_error_message,
   case
       when len(hard_error_message_) > 0 then 'N'
       when len(soft_error_message_) > 0 then 'Y'
       else Null
   end as reprocess_flag,
   trim(
       concat(
           nvl(hard_error_message_, ''),
           nvl(soft_error_message_, '')
       )
   ) as error_message,
   case
       when a.original_cycle_date is not null
        and a.original_batch_id is not null
        and (
             cast('{cycle_date}' as date) != cast(a.original_cycle_date as date)
             or cast({batchid} as int) != cast(a.original_batch_id as int)
            )
        and len(error_message) = 0 then 'Good - Reprocessed Error'
       when a.original_cycle_date is not null
        and a.original_batch_id is not null
        and len(error_message) > 0
        and len(hard_error_message_) > 0 then 'Hard Error'
       when a.original_cycle_date is not null
        and a.original_batch_id is not null
        and len(error_message) > 0
        and len(soft_error_message_) > 0 then 'Soft Error'
       when a.original_cycle_date is not null
        and a.original_batch_id is not null
        and len(error_message) = 0 then 'Good - Current Load'
       when a.original_cycle_date is not null
        and a.original_batch_id is not null
        and len(error_message) > 0 then 'Unknown Error'
       else 'Unknown'
   end as error_cleared,
   sbldgr_include_exclude,
   ldgr_include_exclude
from final a
left join force_error_valid b using (
   original_cycle_date,
   original_batch_id,
   gl_application_area_code,
   gl_source_code_drvd
)


