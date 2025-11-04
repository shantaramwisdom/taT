with input as (
    select
        *,
        from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as original_recorded_timestamp,
        CAST('{cycle_date}' AS DATE) as original_cycle_date,
        CAST({batchid} AS INT) as original_batch_id,
        cast(null as string) as old_error_message,
        cast(null as int) as error_record_aging_days
    from (
        select
            trim(fsi_gl_pt_effective_date) as transaction_date,
            trim(fsi_gl_bh_rev_effective_date) as gl_reversal_date,
            trim(fsi_gl_bh_application_area) as gl_application_area_code,
            trim(fsi_gl_pt_source_code) as gl_source_code,
            trim(fsi_gl_pt_company) as secondary_ledger_code,
            trim(fsi_gl_pt_source_code) as gl_full_source_code,
            trim(fsi_gl_pt_center) as orig_gl_center,
            case
                when trim(fsi_gl_pt_account) like '9%'
                     and not trim(fsi_gl_pt_account) like '99%' then 'Statistical'
                else 'Financial'
            end as data_type
        from {source_database}.fctransstoupload_current
        where cycle_date = {cycledate}

        union

        select
            trim(fsi_gl_pt_effective_date) as transaction_date,
            trim(fsi_gl_bh_rev_effective_date) as gl_reversal_date,
            trim(fsi_gl_bh_application_area) as gl_application_area_code,
            trim(fsi_gl_pt_source_code) as gl_source_code,
            trim(fsi_gl_pt_company) as secondary_ledger_code,
            trim(fsi_gl_pt_source_code) as gl_full_source_code,
            trim(fsi_gl_pt_center) as orig_gl_center,
            case
                when trim(fsi_gl_pt_account) like '9%'
                     and not trim(fsi_gl_pt_account) like '99%' then 'Statistical'
                else 'Financial'
            end as data_type
        from {source_database}.fctransmainframe_current
        where cycle_date = {cycledate}

        union

        select
            trim(fsi_gl_pt_effective_date) as transaction_date,
            trim(fsi_gl_bh_rev_effective_date) as gl_reversal_date,
            trim(fsi_gl_bh_application_area) as gl_application_area_code,
            trim(fsi_gl_pt_source_code) as gl_source_code,
            trim(fsi_gl_pt_company) as secondary_ledger_code,
            trim(fsi_gl_pt_source_code) as gl_full_source_code,
            trim(fsi_gl_pt_center) as orig_gl_center,
            case
                when trim(fsi_gl_pt_account) like '9%'
                     and not trim(fsi_gl_pt_account) like '99%' then 'Statistical'
                else 'Financial'
            end as data_type
        from {source_database}.fctransdistributed_current
        where cycle_date = {cycledate}
    )
    union all
    select
        transaction_date,
        gl_reversal_date,
        gl_application_area_code,
        gl_source_code,
        secondary_ledger_code,
        gl_full_source_code,
        orig_gl_center,
        data_type,
        original_recorded_timestamp,
        original_cycle_date,
        original_batch_id,
        error_message,
        error_record_aging_days
    from error_header
),

master_temp as (
    select
        transaction_date,
        gl_reversal_date,
        gl_application_area_code,
        gl_source_code,
        secondary_ledger_code,
        gl_full_source_code,
        orig_gl_center,
        data_type,

        case
            when gl_source_code is null
                 or len(trim(substr(gl_source_code, 1, 3))) < 3
                 or substr(gl_source_code, 1, 3) like '%x%' then null
            else substr(gl_source_code, 1, 3)
        end as gl_source_code_drvd,

        case
            when secondary_ledger_code is null
                 or len(trim(substr(secondary_ledger_code, 3, 2))) < 2 then null
            else substr(secondary_ledger_code, 3, 2)
        end as secondary_ledger_code_drvd,

        c.oracle_fah_ldgr_nm as ledger_name_drvd,
        b.src_sys_nm_desc as source_system_nm_drvd,
        b.evnt_typ_cd as event_type_code_drvd,
        b.oracle_fah_sblgdr_nm as subledger_short_name_drvd,

        python_date_format_checker(transaction_date, 'YYYY') as transaction_date_chk,
        python_date_format_checker(gl_reversal_date, 'YYYY', True) as gl_reversal_date_chk,

        case
            when transaction_date_chk = 'true' then to_date(transaction_date, 'YYYYDDD')
        end as transaction_date_drvd,

        case
            when gl_reversal_date_chk = 'true' then to_date(gl_reversal_date, 'YYYYDDD')
        end as gl_reversal_date_drvd,

        case
            when substr(gl_full_source_code, 1, 3) in ('903', '851', '444') then nvl(d.reinsurance_assumed_ceded_flag, '@')
        end as reinsuranceassumedcededflag_drvd,

        decode(reinsuranceassumedcededflag_drvd, '@', 1, 0) as reinsuranceassumedcededflag_error_flag,

        cast(original_recorded_timestamp as timestamp) as original_recorded_timestamp,
        cast(original_cycle_date as date) as original_cycle_date,
        cast(original_batch_id as int) as original_batch_id,

        b.include_exclude as sblgdr_include_exclude,
        c.include_exclude as ldgr_include_exclude,

        old_error_message,
        error_record_aging_days
    from input a
    left join lkp_actvty_gl_sblgdr_nm b
        on a.gl_application_area_code = b.ACTVTY_GL_APP_AREA_CD
       and substr(gl_source_code, 1, 3) = b.ACTVTY_GL_SRC_CD
    left join lkp_actvty_gl_ldgr_nm c
        on substr(secondary_ledger_code, 3, 2) = c.actvty_gl_ldgr_cd
       and secondary_ledger_code = c.sec_ldgr_cd
    left join lkp_reinsuranceattributes d
        on substr(a.secondary_ledger_code, 1, 2) = d.legal_entity
       and a.orig_gl_center = d.geac_centers
    where nvl(b.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
      and nvl(c.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
),

master as (
    select
        a.*,
        max(reinsuranceassumedcededflag_error_flag) over (
            partition by ledger_name_drvd,
                         source_system_nm_drvd,
                         event_type_code_drvd,
                         subledger_short_name_drvd,
                         gl_source_code_drvd,
                         gl_application_area_code,
                         transaction_date_drvd,
                         data_type,
                         gl_reversal_date_drvd,
                         secondary_ledger_code_drvd,
                         original_cycle_date,
                         original_batch_id
        ) as max_reinsuranceassumedcededflag_error_flag
    from master_temp a
),

keys as (
    select
        *,
        substr(event_type_code_drvd, 1, 6)
            || date_format(from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central'), 'yyyyMMddHHmmssSSS')
            || '_' || row_number() over (partition by subledger_short_name_drvd order by null) as transaction_number_drvd
    from (
        select distinct
            ledger_name_drvd,
            source_system_nm_drvd,
            event_type_code_drvd,
            subledger_short_name_drvd,
            gl_source_code_drvd,
            gl_application_area_code,
            transaction_date_drvd,
            data_type,
            gl_reversal_date_drvd,
            secondary_ledger_code_drvd,
            original_cycle_date,
            original_batch_id
        from master
        where ledger_name_drvd is not null
          and event_type_code_drvd is not null
          and source_system_nm_drvd is not null
          and subledger_short_name_drvd is not null
          and transaction_date_chk = 'true'
          and gl_reversal_date_chk = 'true'
          and substr(gl_source_code, 1, 3) not like '% %'
          and substr(secondary_ledger_code, 3, 2) not like '% %'
          and len(gl_application_area_code) > 0
          and len(gl_source_code) >= 3
          and len(secondary_ledger_code) >= 4
          and max_reinsuranceassumedcededflag_error_flag = 0
          and (
              ( cast(nvl(gl_reversal_date, 0) as INTEGER) > 0
                and cast(nvl(transaction_date, 0) as INTEGER) <= cast(nvl(gl_reversal_date, 0) as INTEGER) )
              or cast(nvl(gl_reversal_date, 0) as INTEGER) = 0
          )
    )
),

final as (
    select
        a.*,
        b.transaction_number_drvd,

        trim(
            case
                when transaction_date_chk = 'false' then nvl(transaction_date, '') || ' transaction_date is in invalid format, expected YYYYJJJ;'
                else ''
            end
            || case
                when gl_reversal_date_chk = 'false' then nvl(gl_reversal_date, '') || ' gl_reversal_date is in invalid format, expected YYYYJJJ;'
                else ''
            end
            || case
                when cast(nvl(gl_reversal_date, 0) as int) > 0
                     and cast(nvl(transaction_date, 0) as int) > cast(nvl(gl_reversal_date, 0) as int) then
                    nvl2(a.transaction_date_drvd, a.transaction_date_drvd, a.transaction_date, 'NULL')
                    || ' > ' ||
                    nvl2(a.gl_reversal_date_drvd, a.gl_reversal_date_drvd, gl_reversal_date, 'NULL')
                    || ' transaction_date is greater than gl_reversal_date;'
                else ''
            end
        ) as date_error_message_,

        trim(
            nvl(date_error_message_, '')
            || case
                when a.gl_application_area_code is null
                     or len(a.gl_application_area_code) = 0
                     then nvl(a.gl_application_area_code, 'BLANK/NULL') || ' gl_application_area_code is Invalid from Source;'
                else ''
            end
            || case
                when a.gl_source_code is null
                     or substr(a.gl_source_code, 1, 3) like '% %'
                     or len(a.gl_source_code) < 3
                     then nvl(a.gl_source_code, 'BLANK/NULL') || ' gl_source_code is Invalid from Source;'
                else ''
            end
            || case
                when a.secondary_ledger_code is null
                     or substr(a.secondary_ledger_code, 3, 2) like '% %'
                     or len(a.secondary_ledger_code) < 4
                     then nvl(a.secondary_ledger_code, 'BLANK/NULL') || ' secondary_ledger_code is Invalid from Source;'
                else ''
            end
        ) as hard_error_message_,

        trim(
            case
                when len(hard_error_message_) = 0 then
                    (case
                        when a.source_system_nm_drvd is null
                             or a.event_type_code_drvd is null then
                            'source_system_nm/event_type_code is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                            || nvl(substr(gl_source_code, 1, 3), '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                        else ''
                    end)
                    || (case
                        when a.ledger_name_drvd is null then
                            'ledger_name is missing in RDM table lkp_actvty_gl_ldgr_nm for key combination actvty_gl_ldgr_cd/secondary_ledger_code_drvd ('
                            || nvl(substr(secondary_ledger_code, 3, 2), '') || ') and secondary_ledger_code (' || nvl(secondary_ledger_code, '') || ');'
                        else ''
                    end)
                    || (case
                        when a.subledger_short_name_drvd is null then
                            'subledger_short_name is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                            || nvl(a.gl_application_area_code, '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                        else ''
                    end)
                    || (case
                        when b.transaction_number_drvd is null then 'transaction_number is invalid;'
                        else ''
                    end)
                    || (case
                        when a.reinsuranceassumedcededflag_drvd = '@' then
                            'reinsuranceassumedcededflag is missing in RDM table lkp_reinsurance_attributes for key combination secondary_ledger_code ('
                            || nvl(substr(a.secondary_ledger_code, 1, 2), '') || ') and orig_gl_center (' || nvl(orig_gl_center, '') || ');'
                        else ''
                    end)
                else ''
            end
        ) as soft_error_message_,

        case
            when len(hard_error_message_) > 0 then 'N'
            when len(soft_error_message_) > 0 then 'Y'
        end as reprocess_flag,

        trim(
            concat(
                nvl(hard_error_message_, ''),
                nvl(soft_error_message_, '')
            )
        ) as error_message_,

        sblgdr_include_exclude,
        ldgr_include_exclude
    from master a
    left join keys b
        on a.source_system_nm_drvd = b.source_system_nm_drvd
       and a.event_type_code_drvd = b.event_type_code_drvd
       and a.ledger_name_drvd = b.ledger_name_drvd
       and a.subledger_short_name_drvd = b.subledger_short_name_drvd
       and a.transaction_date_drvd = b.transaction_date_drvd
       and a.gl_application_area_code = b.gl_application_area_code
       and a.gl_source_code_drvd = b.gl_source_code_drvd
       and a.secondary_ledger_code_drvd = b.secondary_ledger_code_drvd
       and a.data_type = b.data_type
       and coalesce(a.gl_reversal_date_drvd, '') = coalesce(b.gl_reversal_date_drvd, '')
       and a.original_cycle_date = b.original_cycle_date
       and a.original_batch_id = b.original_batch_id
),

force_error_valid as (
    select distinct
        original_cycle_date,
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
        group by gl_application_area_code, gl_source_code_drvd
    )
      and len(error_message_) = 0
      and original_cycle_date = '{cycle_date}'
      and original_batch_id = {batchid}
)

select
    transaction_date,
    gl_reversal_date,
    gl_application_area_code,
    gl_source_code,
    secondary_ledger_code,
    data_type,
    gl_source_code_drvd,
    gl_full_source_code,
    orig_gl_center,
    reinsuranceassumedcededflag_drvd,
    secondary_ledger_code_drvd,
    ledger_name_drvd,
    source_system_nm_drvd,
    event_type_code_drvd,
    subledger_short_name_drvd,
    transaction_date_chk,
    gl_reversal_date_chk,
    a.transaction_date_drvd,
    gl_reversal_date_drvd,
    transaction_number_drvd,
    original_recorded_timestamp,
    original_cycle_date,
    original_batch_id,
    old_error_message,
    error_record_aging_days,
    sblgdr_include_exclude,
    ldgr_include_exclude,
    trim(
        case
            when len(hard_error_message_) > 0 then (
                case
                    when a.source_system_nm_drvd is null
                         or a.event_type_code_drvd is null then
                        'source_system_nm/event_type_code is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                        || nvl(substr(gl_source_code, 1, 3), '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                    else ''
                end
                || case
                    when a.ledger_name_drvd is null then
                        'ledger_name is missing in RDM table lkp_actvty_gl_ldgr_nm for key combination actvty_gl_ldgr_cd/secondary_ledger_code_drvd ('
                        || nvl(substr(secondary_ledger_code, 3, 2), '') || ') and secondary_ledger_code (' || nvl(secondary_ledger_code, '') || ');'
                    else ''
                end
                || case
                    when a.subledger_short_name_drvd is null then
                        'subledger_short_name is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                        || nvl(substr(gl_source_code, 1, 3), '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                    else ''
                end
                || case
                    when b.transaction_number_drvd is null then 'transaction_number is invalid;'
                    else ''
                end
                || case
                    when a.reinsuranceassumedcededflag_drvd = '0' then
                        'reinsuranceassumedcededflag is missing in RDM table lkp_reinsurance_attributes for key combination secondary_ledger_code ('
                        || nvl(substr(a.secondary_ledger_code, 1, 2), '') || ') and orig_gl_center (' || nvl(orig_gl_center, '') || ');'
                    else ''
                end
            )
            else ''
        end
    ) as hard_error_message_,

    trim(
        case
            when len(hard_error_message_) = 0 then (
                case
                    when a.source_system_nm_drvd is null
                         or a.event_type_code_drvd is null then
                        'source_system_nm/event_type_code is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                        || nvl(substr(gl_source_code, 1, 3), '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                    else ''
                end
                || case
                    when a.ledger_name_drvd is null then
                        'ledger_name is missing in RDM table lkp_actvty_gl_ldgr_nm for key combination actvty_gl_ldgr_cd/secondary_ledger_code_drvd ('
                        || nvl(substr(secondary_ledger_code, 3, 2), '') || ') and secondary_ledger_code (' || nvl(secondary_ledger_code, '') || ');'
                    else ''
                end
                || case
                    when a.subledger_short_name_drvd is null then
                        'subledger_short_name is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                        || nvl(substr(gl_source_code, 1, 3), '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                    else ''
                end
                || case
                    when b.transaction_number_drvd is null then 'transaction_number is invalid;'
                    else ''
                end
                || case
                    when a.reinsuranceassumedcededflag_drvd = '0' then
                        'reinsuranceassumedcededflag is missing in RDM table lkp_reinsurance_attributes for key combination secondary_ledger_code ('
                        || nvl(substr(a.secondary_ledger_code, 1, 2), '') || ') and orig_gl_center (' || nvl(orig_gl_center, '') || ');'
                    else ''
                end
            )
            else ''
        end
    ) as soft_error_message_,

    case
        when len(hard_error_message_) > 0 then 'N'
        when len(soft_error_message_) > 0 then 'Y'
    end as reprocess_flag,

    trim(
        concat(
            nvl(hard_error_message_, ''),
            nvl(soft_error_message_, '')
        )
    ) as error_message_,

    a.transaction_number_drvd,
    trim(
        case
            when b.original_cycle_date is not null
                 and len(hard_error_message_) = 0 then
                'Forced Error. Date Errors Present in Other Headers for combination of gl_application_area_code ('
                || nvl(a.gl_application_area_code, '') || ') and gl_source_code (' || nvl(a.gl_source_code_drvd, '') || ');'
            else ''
        end || nvl(hard_error_message_, '')
    ) as hard_error_message_forced,

    soft_error_message_ as soft_error_message,
    case
        when len(hard_error_message_) > 0 then 'N'
        when len(soft_error_message_) > 0 then 'Y'
    end as reprocess_flag_forced,

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
         and len(hard_error_message) > 0 then 'Hard Error'
        when a.original_cycle_date is not null
         and a.original_batch_id is not null
         and len(error_message) > 0
         and len(soft_error_message) > 0 then 'Soft Error'
        when a.original_cycle_date is not null
         and a.original_batch_id is not null
         and len(error_message) = 0 then 'Good - Current Load'
        when a.original_cycle_date is not null
         and a.original_batch_id is not null
         and len(error_message) > 0 then 'Unknown Error'
        else 'Unknown'
    end as error_cleared,

    sblgdr_include_exclude,
    ldgr_include_exclude
from final a
left join force_error_valid b using (
    original_cycle_date,
    original_batch_id,
    gl_application_area_code,
    gl_source_code_drvd
);