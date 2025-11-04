with input as (
    select *,
           from_utc_timestamp(current_timestamp(),'US/Central') as original_recorded_timestamp,
           cast('{cycle_date}' as date) as original_cycle_date,
           cast({batchid} as int) as original_batch_id,
           cast(null as string) as old_error_message,
           cast(null as int) as error_record_aging_days
    from (
          select trim(i_fsi_gl_pt_effect_dte) as transaction_date,
                 case
                     when trim(i_fsi_gl_bh_data_type_n) = '2' then 'Financial'
                     when trim(i_fsi_gl_bh_data_type_n) = '3' then 'Statistical'
                     else null
                 end as data_type,
                 trim(i_fsi_gl_pt_source) as gl_source_code,
                 trim(i_fsi_gl_bh_pp_area) as gl_application_area_code,
                 trim(i_fsi_gl_bh_project_code) as contractnumber
          from {source_database}.dbfile_current
          where cycle_date = '{cycledate}'
          union
          select trim(i_fsi_gl_pt_effect_dte) as transaction_date,
                 case
                     when trim(i_fsi_gl_bh_data_type_n) = '2' then 'Financial'
                     when trim(i_fsi_gl_bh_data_type_n) = '3' then 'Statistical'
                     else null
                 end as data_type,
                 trim(i_fsi_gl_pt_source) as gl_source_code,
                 trim(i_fsi_gl_bh_pp_area) as gl_application_area_code,
                 trim(i_fsi_gl_bh_sus_code) as contractnumber
          from {source_database}.dbfile2_current
          where cycle_date = '{cycledate}'
         )
    union all
    select transaction_date,
           data_type,
           gl_source_code,
           secondary_ledger_code,
           gl_application_area_code,
           contractnumber,
           original_recorded_timestamp,
           original_cycle_date,
           original_batch_id,
           error_message,
           error_record_aging_days
    from error_header
),
master as (
    select transaction_date,
           data_type,
           gl_source_code,
           secondary_ledger_code,
           gl_application_area_code,
           contractnumber,
           case
               when gl_source_code is null or len(trim(substr(gl_source_code,1,3))) < 3 or substr(gl_source_code,1,3) like 'XX%' then null
               else substr(gl_source_code,3,2)
           end as gl_source_code_drvd,
           case
               when secondary_ledger_code is null or len(trim(substr(secondary_ledger_code,3,2))) < 2 then null
               else substr(secondary_ledger_code,3,2)
           end as secondary_ledger_code_drvd,
           c.oracle_fah_ldgr_nm as ledger_name_drvd,
           b.src_sys_m_desc as source_system_drvd,
           d.evnt_typ_cd as event_type_cd_drvd,
           b.oracle_fah_sblgdr_nm as subledger_short_name_drvd,
           python_date_fmt_checker(transaction_date,'YYYY') as transaction_date_chk,
           case
               when transaction_date_chk = 'true' then to_date(transaction_date,'yyyyDDD')
               else null
           end as transaction_date_drvd,
           cast(original_recorded_timestamp as timestamp) as original_recorded_timestamp,
           cast(original_cycle_date as date) as original_cycle_date,
           cast(original_batch_id as int) as original_batch_id,
           b.include_exclude as sblgdr_include_exclude,
           c.include_exclude as ldgr_include_exclude,
           old_error_message,
           error_record_aging_days
    from input i
    left join lkp_actvty_gl_sblggr_nm b on (gl_application_area_code = b.actvty_gl_app_area_cd and substr(gl_source_code,1,3) = b.actvty_gl_src_cd)
    left join lkp_actvty_ldgr_nm c on (substr(secondary_ledger_code,3,2) = c.actvty_gl_ldgr_cd)
    where nvl(c.include_exclude,'NULLVAL') in ('Include','NULLVAL')
),
keys as (
    select substr(event_type_code_drvd,1,6) || date_format(from_utc_timestamp(current_timestamp(),'US/Central'),'yyyyMMddHHmmss') || row_number() over(partition by subledger_short_name_drvd order by null
    ) as transaction_number_drvd
    from (
          select distinct ledger_name_drvd,
                          source_system_nm_drvd,
                          event_type_code_drvd,
                          subledger_short_name_drvd,
                          contractnumber,
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
          and substr(gl_source_code,3,3) not like 'X X'
          and substr(secondary_ledger_code,3,2) not like 'X X'
          and len(gl_application_area_code) > 0
          and len(gl_source_code) >= 3
          and len(secondary_ledger_code) >= 4
         )
),
final as (
    select a.*,
           b.transaction_number_drvd,
           trim(
           case
               when transaction_date_chk = 'false' then nvl(transaction_date,'') || ' transaction_date is in invalid format, expected YYYY-MM-DD;'
               else ''
           end
           ) as date_error_message,
           trim(
           nvl(date_error_message,'') || case
               when a.gl_application_area_code is null or len(a.gl_application_area_code) = 0 then nvl(a.gl_application_area_code,'BLANK/NULL') || ' gl_application_area_code is Invalid from Source;'
               else ''
           end || case
               when a.gl_source_code is null or substr(a.gl_source_code,3,3) like 'X X' or len(a.gl_source_code) < 3 then nvl(a.gl_source_code,'BLANK/NULL') || ' gl_source_code is Invalid from Source;'
               else ''
           end || case
               when a.secondary_ledger_code is null or substr(a.secondary_ledger_code,3,2) like 'X X' or len(a.secondary_ledger_code) < 4 then nvl(a.secondary_ledger_code,'BLANK/NULL') || ' secondary_ledger_code is Invalid from Source;'
               else ''
           end
           ) as hard_error_message,
           trim(
           case
               when len(hard_error_message) = 0 then case
                   when a.source_system_nm_drvd is null or a.event_type_code_drvd is null then 'source_system_nm/event_type_code is missing in RDM table lkp_actvty_gl_sblggr_nm for key combination gl_application_area_code (' || nvl(a.gl_application_area_code,'') || ') and gl_source_code (' || nvl(substr(gl_source_code,1,3),'') || ');'
                   else ''
               end || case
                   when a.ledger_name_drvd is null then 'ledger_name is missing in RDM table lkp_actvty_ldgr_nm for key combination actvty_gl_ldgr_cd/secondary_ledger_code_drvd (' || nvl(substr(secondary_ledger_code,3,2),'') || ');'
                   else ''
               end || case
                   when a.subledger_short_name_drvd is null then 'subledger_short name is missing in RDM table lkp_actvty_gl_sblggr_nm for key combination gl_application_area_code (' || nvl(a.gl_application_area_code,'') || ') and gl_source_code (' || nvl(substr(gl_source_code,1,3),'') || ');'
                   else ''
               end || case
                   when b.transaction_number_drvd is null then 'transaction_number is invalid;'
                   else ''
               end
           end
           ) as soft_error_message,
           case
               when len(hard_error_message) > 0 then 'N'
               when len(soft_error_message) > 0 then 'Y'
               else null
           end as reprocess_flag,
           trim(
               concat(
                   nvl(hard_error_message,''),
                   nvl(soft_error_message,'')
               )
           ) as error_message,
           sblgdr_include_exclude,
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
                    and coalesce(a.contractnumber,'') = coalesce(b.contractnumber,'')
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
            where len(date_error_message) > 0
            and original_cycle_date = '{cycle_date}'
            and original_batch_id = {batchid}
            group by gl_application_area_code,
                     gl_source_code_drvd
          )
    and len(error_message) = 0
    and original_cycle_date = '{cycle_date}'
    and original_batch_id = {batchid}
)
select data_type,
       event_type_code_drvd,
       gl_application_area_code,
       gl_source_code,
       gl_source_code_drvd,
       ledger_name_drvd,
       secondary_ledger_code,
       secondary_ledger_code_drvd,
       source_system_nm_drvd,
       subledger_short_name_drvd,
       transaction_date,
       contractnumber,
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
           and len(hard_error_message) = 0 then 'Forced Error. Date Errors Present in Other Headers for combination of gl_application_area_code (' || nvl(a.gl_application_area_code,'') || ') and gl_source_code (' || nvl(a.gl_source_code_drvd,'') || ');'
           else ''
       end
       ) as hard_error_message,
       soft_error_message,
       case
           when len(hard_error_message) > 0 then 'N'
           when len(soft_error_message) > 0 then 'Y'
           else null
       end as reprocess_flag,
       trim(
           concat(
               nvl(hard_error_message,''),
               nvl(soft_error_message,'')
           )
       ) as error_message,
       case
           when a.original_cycle_date is not null
           and a.original_batch_id is not null
           and (cast('{cycle_date}' as date) != cast(a.original_cycle_date as date)
           or cast({batchid} as int) != cast(a.original_batch_id as int))
           and len(error_message) = 0 then 'Good - Reprocessed Error'
           when a.original_cycle_date is not null
           and a.original_batch_id is not null
           and len(hard_error_message) > 0 then 'Hard Error'
           when a.original_cycle_date is not null
           and a.original_batch_id is not null
           and len(soft_error_message) > 0 then 'Soft Error'
           when a.original_cycle_date is not null
           and a.original_batch_id is not null
           and len(error_message) = 0 then 'Good - Current Load'
           when a.original_cycle_date is not null
           and a.original_batch_id is not null
           and len(error_message) > 0 then 'Unknown Error'
           else 'Unknown'
       end as error_class,
       sblgdr_include_exclude,
       ldgr_include_exclude
from final a
left join force_error_valid b using(
       original_cycle_date,
       original_batch_id,
       gl_application_area_code,
       gl_source_code_drvd
)