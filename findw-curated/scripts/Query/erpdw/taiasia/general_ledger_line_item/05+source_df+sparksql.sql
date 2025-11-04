with current_run_all_valid_headers as (
    select original_cycle_date,
           original_batch_id,
           transaction_number,
           ledger_name,
           contractnumber,
           transaction_date,
           data_type,
           secondary_ledger_code,
           source_system_nm,
           event_type_code,
           subledger_short_name
      from {curated_database}.{source_system_name}_general_ledger_header
     where cycle_date = '{cycle_date}'
       and batch_id = {batchid}
       and original_cycle_date is not null
       and original_batch_id is not null
       and ( original_batch_id != {batchid}
          or original_cycle_date != '{cycle_date}' )
    union all
    select cycle_date,
           batch_id,
           transaction_number,
           ledger_name,
           contractnumber,
           transaction_date,
           data_type,
           secondary_ledger_code,
           source_system_nm,
           event_type_code,
           subledger_short_name
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
            select trim(CIREC802_POL_FILE_DATE) as transaction_date,
                   trim(CIREC802_pol_le) || trim(CIREC802_pol_lg) as secondary_ledger_code,
                   'Financial' as data_type,
                   trim(CIREC802_POL_REF_NUMBER) as contractnumber,
                   'LifePro119' as contractsourcesystemname,
                   case
                        when trim(CIREC802_POL_TRANS_AMOUNT) >= 0 then 'Debit'
                        when trim(CIREC802_POL_TRANS_AMOUNT) < 0 then 'Credit'
                        else NULL
                   end as debit_credit_indicator,
                   trim(CIREC802_POL_TRANS_AMOUNT) as default_amount,
                   trim(CIREC802_POL_GEAC_ACCT_NUM)||trim(CIREC802_POL_LOB) as orig_gl_account,
                   trim(CIREC802_POL_GEAC_CENTER) as orig_gl_center,
                   trim(CIREC802_POL_LE) || trim(CIREC802_POL_lg) as orig_gl_company,
                   'None' as ifrs17reportingcashflowtype,
                   trim(CIREC802_POL_RES_STATE) as statutoryresidentstatecode
              from {source_database}.accounting_detail_current
             where cycle_date = {cycledate}
           )
    union all
    select transaction_date,
           secondary_ledger_code,
           data_type,
           contractnumber,
           contractsourcesystemname,
           debit_credit_indicator,
           default_amount,
           orig_gl_account,
           orig_gl_center,
           orig_gl_company,
           statutoryresidentstatecode,
           ifrs17reportingcashflowtype,
           original_recorded_timestamp,
           original_cycle_date,
           original_batch_id,
           error_message,
           error_record_aging_days
      from error_line_item
),
master as (
    select a.*,
           substr(secondary_Ledger_code, 3, 2) as secondary_Ledger_code_drvd,
           --
           nvl(
                cast(
                        case
                            when cast(nvl(default_amount, 0) as numeric(18,2)) <= 0 then 0
                            else cast(nvl(default_amount, 0) as numeric(18,2))
                        end as decimal(18, 2)
                    ), 0
              ) as default_amount_drvd,
           abs(nvl(cast(default_amount as decimal(18, 2)),0.0)) as default_amount_drvd,
           c.oracle_fah_ldgr_nm as ledger_name_drvd,
           'TAI ASIA' as source_system_nm_drvd,
           b.event_type_code as event_type_code_drvd,
           b.oracle_fah_subldgr_nm as subledger_short_name_drvd,
           python_date_format_checker(transaction_date, '%m/%d/%Y') as transaction_date_chk,
           case
                when transaction_date_chk = 'true' then to_date(transaction_date, 'MM/dd/yyyy')
                else null
           end as transaction_date_drvd,
           case
                when statutoryresidentstatecode is null then null
                when d.src_input is null then '@'
                else e.iso_prvnc_cd
           end as statutoryresidentstatecode_drvd,
           case
                when statutoryresidentstatecode is null then null
                when d.src_input is null then '@'
                else e.iso_cntry_cd
           end as statutoryresidentcountrycode,
           case
                when transaction_date_chk = 'false' then 'Y'
                else 'N'
           end as date_error,
           monotonically_increasing_id() random_counter
      from input a
      left join lkp_actvty_gl_sbldgr_nm b
             on '--' = b.ACTVTY_GL_APP_AREA_CD
            and b.ACTVTY_GL_SRC_CD
            and 'TAI ASIA' = b.src_sys_nm_desc
      left join lkp_src c
             on substr(secondary_Ledger_code, 3, 2) = c.actvty_gl_ldgr_cd
            and secondary_ledger_code = c.sec_ldgr_cd
      left join lkp_grpchy d
             on a.statutoryresidentstatecode = d.src_input
      left join dim_grpchy e using(GRPCHY_ID)
     where nvl(c.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
)
select a.*,
       b.transaction_number,
       trim(
            case
                when transaction_date_chk = 'false' then 'Header Errored for Date Validation;'
                else ''
            end ||
            case
                when a.original_cycle_date is not null
                 and a.date_error = 'N' then 'Forced Error. Date Errors Present in Other Headers;'
                else ''
            end ||
            case
                when a.secondary_ledger_code is null
                 or a.secondary_ledger_code like '%'
                 or len(a.secondary_ledger_code) < 2 then nvl(a.secondary_ledger_code, 'BLANK/NULL') ||
                                                        ' secondary_ledger_code is Invalid from Source;'
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
                    )
                     then 'Header Errored for RDM Lookup;'
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
                                                             statutoryresidentstatecode )
            else null
       end as line_number
  from master a
  left join current_run_all_valid_headers b
         on a.original_cycle_date = b.original_cycle_date
        and a.original_batch_id = b.original_batch_id
        and a.source_system_nm_drvd = b.source_system_nm
        and a.event_type_code_drvd = b.event_type_code
        and a.ledger_name_drvd = b.ledger_name
        and a.subledger_short_name_drvd = b.subledger_short_name
        and a.transaction_date_drvd = b.transaction_date
        and a.secondary_ledger_code_drvd = b.secondary_ledger_code
        and a.data_type = b.data_type
        and coalesce(a.contractnumber, '') = coalesce(b.contractnumber, '')
  left join (
            select distinct original_cycle_date,
                            original_batch_id
              from current_run_forced_header_errors
           ) c
         on a.original_cycle_date = c.original_cycle_date
        and a.original_batch_id = c.original_batch_id
