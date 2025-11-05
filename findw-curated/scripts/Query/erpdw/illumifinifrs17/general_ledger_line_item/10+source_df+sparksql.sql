with current_run_all_valid_headers as (
    select original_cycle_date,
           original_batch_id,
           transaction_number,
           ledger_name,
           gl_application_area_code,
           sourcelegalentity,
           transaction_date,
           data_type,
           secondary_ledger_code,
           source_system_nm,
           event_type_code,
           subledger_short_name,
           gl_source_code
    from {curated_database}.{source_system_name}_general_ledger_header
    where cycle_date = '{cycle_date}'
      and batch_id = {batchid}
      and original_cycle_date is not null
      and original_batch_id is not null
      and (
            original_batch_id != {batchid}
         or original_cycle_date != '{cycle_date}'
          )
    union all
    select cycle_date,
           batch_id,
           transaction_number,
           ledger_name,
           gl_application_area_code,
           sourcelegalentity,
           transaction_date,
           data_type,
           secondary_ledger_code,
           source_system_nm,
           event_type_code,
           subledger_short_name,
           gl_source_code
    from {curated_database}.{source_system_name}_general_ledger_header
    where cycle_date = '{cycle_date}'
      and batch_id = {batchid}
      and original_cycle_date is null
      and original_batch_id is null
),
input as (
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
               trim(policy) as contractnumber,
               case when trim(sub_sys_code) = 'FLX' then 'FLEX'
                    when trim(sub_sys_code) = 'EST' then 'ESTATE'
                    when trim(sub_sys_code) = 'HIP' then 'MIPS'
                    when trim(sub_sys_code) = 'ONX' then 'ONYX'
                    when trim(sub_sys_code) = 'SCD' then 'SCHED'
                    when trim(sub_sys_code) = 'FID' then 'FIDELITY'
                    else null
                end as contractsourcesystemname,
               trim(state) as statutoryresidentstatecode,
               trim(amount) as default_amount,
               case
                   when trim(debit_credit) = 'D' then 'Debit'
                   when trim(debit_credit) = 'C' then 'Credit'
                   else null
               end as debit_credit_indicator,
               trim(source_company) as orig_gl_company,
               trim(product_fund_code) as orig_gl_account,
               trim(product_fund_code) as orig_gl_center,
               trim(transaction_code) as activitysourcectransactioncode,
               concat_ws('_',trim(source_company),trim(account),trim(product_fund_code),trim(geo_tax_code)) as sourcesystemactivitydescription,
               trim(account) as sourcesystemgeneralledgeraccountnumber,
               trim(plan_code) as plancode,
               trim(voucher_no) as checknumber
        from {source_database}.accounting_detail_current
        where cycle_date = {cycledate}
union all
select transaction_date,
       gl_application_area_code,
       gl_source_code,
       secondary_ledger_code,
       data_type,
       contractnumber,
       contractsourcesystemname,
       statutoryresidentstatecode,
       default_amount,
       debit_credit_indicator,
       orig_gl_company,
       orig_gl_account,
       orig_gl_center,
       activitysourcectransactioncode,
       sourcesystemactivitydescription,
       sourcesystemgeneralledgeraccountnumber,
       plancode,
       checknumber,
       original_recorded_timestamp,
       original_cycle_date,
       original_batch_id,
       error_message,
       error_record_aging_days
from error_line_item
),
master as (
    select a.*,
           gl_source_code as gl_source_code_drvd,
           secondary_ledger_code as secondary_ledger_code_drvd,
           nvl(
               cast(
                   cast(
                       nvl(trim(default_amount), 0) as numeric(18, 2)
                   ) as decimal(18, 2)
               ),
               0
           ) as default_amount_drvd,
           c.oracle_fah_ldgr_nm as ledger_name_drvd,
           b.src_sys_nm_desc as source_system_nm_drvd,
           b.evnt_typ_cd as event_type_code_drvd,
           b.oracle_fah_subldgr_nm as subledger_short_name_drvd,
           python_date_format_checker(transaction_date, 'xMxdyY') as transaction_date_chk,
           case
               when transaction_date_chk = 'true' then to_date(transaction_date, 'MMddyyyy')
               else null
           end as transaction_date_drvd,
           case
               when statutoryresidentstatecode is null then null
               when d.src_input is null then '0'
               else e.iso_state_prvnc_cd
           end as statutoryresidentstatecode_drvd,
           case
               when statutoryresidentstatecode is null then null
               when d.src_input is null then '0'
               else e.iso_cntry_cd
           end as statutoryresidentcountrycode_drvd,
           c2.lgl_enty_cd || '01' as orig_gl_company_drvd,
           case
               when orig_gl_center is null then i.cntr_nmbr
               else j.cntr_nmbr
           end as orig_gl_center_drvd,
           case
               when orig_gl_account is null then i.gnrl_ldgr_acct_nmbr || i.gnrl_ldgr_acct_sffx
               else j.gnrl_ldgr_acct_nmbr || j.gnrl_ldgr_acct_sffx
           end as orig_gl_account_drvd,
           case
               when f.origntrng_cntrct_nbr is null then null
               else f.ifrs17_msrnmnt_mdl
           end as ifrs17measurementmodel,
           case
               when f.origntrng_cntrct_nbr is null then null
               else f.ifrs17_portfolio
           end as ifrs17portfolio,
           case
               when f.origntrng_cntrct_nbr is null then null
               else f.ifrs17_cohort
           end as ifrs17cohort,
           case
               when f.origntrng_cntrct_nbr is null then null
               else f.ifrs17_prftblty
           end as ifrs17profitability,
           case
               when f.origntrng_cntrct_nbr is null then null
               else f.ifrs17_grpng
           end as ifrs17grouping,
           case
               when substring(orig_gl_account, 1) in ('1', '2', '3') then 'NONE'
               when substring(orig_gl_account, 1) in ('4', '5', '6', '7', '8')
                    and g.ifrs4_center is null then 'NONE'
               when substring(orig_gl_account, 1) in ('4', '5', '6', '7', '8')
                    and g.ifrs4_center is not null
                    and h.center_block_foracctmapping is not null then h.reporting_cashflowtype
               else 'NONE'
           end as ifrs17reportingcashflowtype,
           case
               when transaction_date_chk = 'false' then 'Y'
               else 'N'
           end as date_error,
           monotonically_increasing_id() random_counter
    from input a
             left join lkp_actvty_gl_sbldgr_nm b on a.gl_application_area_code = b.ACTVTY_GL_APP_AREA_CD
                 and a.gl_source_code = b.ACTVTY_GL_SRC_CD
             left join lkp_lgl_enty_cd c2 on c2.cntrt_src_sys_nm = 'illumifin' and c2.src_lgl_enty_cd = a.orig_gl_company
             left join lkp_actvty_ldgr_nm c on c.actvty_gl_ldgr_cd = a.orig_gl_center
             left join lkp_src_gegrphy d on a.statutoryresidentstatecode = src_input
             left join dim_gegrphy e on e.GEGRPHY_ID = d.GEGRPHY_ID
             left join lkp_ifrs17_cntrt_tag_vw f on a.contractnumber = f.origntrng_cntrct_nbr
             left join rr_rs01_cntr_to_cntr_mapping g on substring(orig_gl_account, 1) in ('4', '5', '6', '7', '8')
                 and a.orig_gl_center = g.ifrs4_center
             left join rr_rs03_acct_to_crt_mapping h on substring(a.orig_gl_account, 1, 5) = h.account_number
                 and h.center_block_foracctmapping = g.center_block_foracctmapping
             left join lkp_src_acct_to_gl_acct i on i.src_lgl_enty = a.orig_gl_company
                 and i.src_sys_acct_nmbr = a.sourcesystemgeneralledgeraccountnumber
                 and i.src_sys_prdct_cd = a.orig_gl_center
             left join lkp_src_acct_to_gl_acct j on j.src_lgl_enty = a.orig_gl_company
                 and j.src_sys_acct_nmbr = a.sourcesystemgeneralledgeraccountnumber
                 and j.src_sys_prdct_cd = a.orig_gl_center
    where nvl(c.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
      and a.orig_gl_company in ('301', '501', '701')
)
select a.*,
       b.transaction_number,
       trim(
           case
               when transaction_date_chk = 'false' then 'Header Errored for Date Validation;'
               else ''
           end || case
                      when c.original_cycle_date is not null
                           and a.date_error = 'N' then 'Forced Error. Date Errors Present in Other Headers for combination of gl_application_area_code (' || nvl(a.gl_application_area_code, '') || ') and gl_source_code (' || nvl(a.gl_source_code_drvd, '') || '):'
                      else ''
                  end
       ) as hard_error_message,
       trim(
           case
               when len(trim(hard_error_message)) = 0
                    and (
                        b.transaction_number is null
                        or source_system_nm_drvd is null
                        or ledger_name_drvd is null
                        or event_type_code_drvd is null
                        or subledger_short_name_drvd is null
                    ) then 'Header Errored for RDM Lookup:'
               else ''
           end || case
                      when a.orig_gl_center_drvd is Null then 'orig_gl_center is missing in RDM lkp_src_acct_to_gl_acct for orig_gl_center (' || nvl(a.orig_gl_center, '') || ');'
                      else ''
                  end || case
                      when a.orig_gl_company_drvd is Null then 'orig_gl_company is missing in RDM lkp_lgl_enty_cd for orig_gl_company (' || nvl(a.orig_gl_company, '') || ');'
                      else ''
                  end || case
                      when a.orig_gl_account_drvd is Null then 'orig_gl_account is missing in RDM lkp_src_acct_to_gl_acct for orig_gl_account (' || nvl(a.orig_gl_account, '') || ');'
                      else ''
                  end
       ) as soft_error_message,
       trim(
           concat(
               nvl(hard_error_message, ''),
               nvl(soft_error_message, '')
           )
       ) as error_message,
       '{source_system_name}' as source_system_name,
       case
           when len(error_message) = 0 then row_number() over (
               partition by b.transaction_number
               order by default_amount_drvd,
                        debit_credit_indicator,
                        orig_gl_company,
                        orig_gl_account,
                        orig_gl_center,
                        sourcesystemactivitydescription,
                        statutoryresidentstatecode,
                        ifrs17cohort,
                        ifrs17grouping,
                        ifrs17measurementmodel,
                        ifrs17portfolio,
                        ifrs17profitability,
                        ifrs17reportingcashflowtype,
                        sourcesystemgeneralledgeraccountnumber,
                        plancode,
                        checknumber
           )
           else null
       end as line_number
from master a
         left join current_run_all_valid_headers b on a.original_cycle_date = b.original_cycle_date
             and a.original_batch_id = b.original_batch_id
             and a.source_system_nm_drvd = b.source_system_nm
             and a.event_type_code_drvd = b.event_type_code
             and a.ledger_name_drvd = b.ledger_name
             and a.subledger_short_name_drvd = b.subledger_short_name
             and a.transaction_date_drvd = b.transaction_date
             and a.gl_application_area_code = b.gl_application_area_code
             and a.gl_source_code_drvd = b.gl_source_code
             and a.secondary_ledger_code_drvd = b.secondary_ledger_code
             and a.data_type = b.data_type
             and a.orig_gl_company = b.sourcelegalentity
         left join (
             select distinct original_cycle_date,
                             original_batch_id,
                             gl_application_area_code,
                             gl_source_code
             from current_run_forced_header_errors
         ) c on a.original_cycle_date = c.original_cycle_date
             and a.original_batch_id = c.original_batch_id
             and a.gl_application_area_code = c.gl_application_area_code
             and a.gl_source_code = c.gl_source_code

