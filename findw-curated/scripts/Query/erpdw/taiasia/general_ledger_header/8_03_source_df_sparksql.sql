with input as (
    select *,
        from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as original_recorded_timestamp,
        CAST('{cycle_date}' AS DATE) as original_cycle_date,
        CAST('{batchid}' AS INT) as original_batch_id,
        cast(null as string) as old_error_message,
        cast(null as int) as error_record_aging_days
    from (
        select 'Financial' as data_type,
            trim(circe802_pol_le) || trim(circe802_pol_lg) as secondary_ledger_code,
            'TAI ASIA' as source_system_nm,
            trim(circe802_pol_file_date) as transaction_date,
            trim(circe802_pol_ref_number) as contractnumber,
            'LifePro119' as contractsourcesystemname
        from {source_database}.accounting_detail_current
        where cycle_date = '{cycledate}'
        group by 1, 2, 3, 4, 5, 6
    )
    union all
    select transaction_date,
        secondary_ledger_code,
        source_system_nm,
        data_type,
        contractnumber,
        contractsourcesystemname,
        original_recorded_timestamp,
        original_cycle_date,
        original_batch_id,
        error_message,
        error_record_aging_days
    from error_header
),
master as (
    select transaction_date,
        secondary_ledger_code,
        data_type,
        source_system_nm,
        contractnumber,
        contractsourcesystemname,
        substr(secondary_ledger_code,3,2) as secondary_ledger_code_drvd,
        c.oracle_fah_ldgr_nm as ledger_name_drvd,
        source_system_nm as source_system_nm_drvd,
        b.event_typ_cd as event_type_code_drvd,
        b.oracle_fah_sublggr_nm as subledger_short_name_drvd,
        python_date_format_checker(transaction_date, '%m/%d/%Y') as transaction_date_chk,
        case
            when transaction_date_chk = 'true' then to_date(transaction_date, 'MM/dd/yyyy')
            else null
        end as transaction_date_drvd,
        cast(original_recorded_timestamp as timestamp) as original_recorded_timestamp,
        cast(original_cycle_date as date) as original_cycle_date,
        cast(original_batch_id as int) as original_batch_id,
        b.include_exclude as sblggr_include_exclude,
        c.include_exclude as ldgr_include_exclude,
        old_error_message,
        error_record_aging_days
    from input a
    left join lkp_actvty_gl_sblggr_nm b on '~' = b.ACTVTY_GL_APP_AREA_CD 
        and '~' = b.ACTVTY_GL_SRC_CD and 'TAI ASIA' = b.src_sys_nm_desc
    left join lkp_actvty_ldgr_nm c on substr(secondary_ledger_code, 3, 2) = c.actvty_gl_ldgr_cd 
        and secondary_ledger_code = c.sec_ldgr_cd
    where nvl(c.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
),
keys as (
    select *,
        substr(event_type_code_drvd, 1, 6) || date_format(
            from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central'),
            'yyyyMMddHHmmssSSS'
        ) || '_' || row_number() over(
            partition by subledger_short_name_drvd
            order by null
        ) as transaction_number_drvd
    from (
        select distinct ledger_name_drvd,
            source_system_nm_drvd,
            event_type_code_drvd,
            subledger_short_name_drvd,
            transaction_date_drvd,
            data_type,
            contractnumber,
            contractsourcesystemname,
            secondary_ledger_code_drvd,
            original_cycle_date,
            original_batch_id
        from master
        where ledger_name_drvd is not null
            and event_type_code_drvd is not null
            and source_system_nm_drvd is not null
            and subledger_short_name_drvd is not null
            and transaction_date_chk = 'true'
            and secondary_ledger_code not like '% %'
            and len(secondary_ledger_code) >= 4
    )
),
final as (
    select a.*,
        trim(
            case
                when nvl(transaction_date_chk,'false') = 'false' then nvl(transaction_date,'') || ': transaction_date is in invalid format, expected yyyymmdd;'
                when len(secondary_ledger_code) < 2 then nvl(secondary_ledger_code,'BLANK/NULL') || ': secondary_ledger_code is invalid from source;'
                when len(secondary_ledger_code) >= 2 then case
                    when ledger_name_drvd is null then 'ledger_name is missing in ROM table lkp_actvty_ldgr_nm for key combination actvty_gl_ldgr_cd/secondary_ledger_code: (' || nvl(substr(secondary_ledger_code,3,2),'') || '/' || nvl(secondary_ledger_code,'') || ');'
                    when subledger_short_name_drvd is null then 'subledger_short_name is missing in ROM table lkp_actvty_gl_sblggr_nm;'
                    when source_system_nm_drvd is null or event_type_code_drvd is null then 'source_system_nm/event_type_code is missing in ROM table lkp_actvty_gl_sblggr_nm;'
                    when transaction_number_drvd is null then 'transaction_number is invalid;'
                    else ''
                end
                else ''
            end
        ) as hard_error_message_,
        case
            when len(hard_error_message_) > 0 then 'N'
            else null
        end as soft_error_message_,
        case
            when len(hard_error_message_) > 0 then 'N'
            when len(soft_error_message_) > 0 then 'Y'
            else null
        end as reprocess_flag,
        trim(
            concat(
                nvl(hard_error_message_,''),
                nvl(soft_error_message_,'')
            )
        ) as error_message_,
        sblggr_include_exclude,
        ldgr_include_exclude
    from master a
    left join keys b on a.source_system_nm_drvd = b.source_system_nm_drvd
        and a.event_type_code_drvd = b.event_type_code_drvd
        and a.ledger_name_drvd = b.ledger_name_drvd
        and a.subledger_short_name_drvd = b.subledger_short_name_drvd
        and a.transaction_date_drvd = b.transaction_date_drvd
        and a.secondary_ledger_code_drvd = b.secondary_ledger_code_drvd
        and a.data_type = b.data_type
        and coalesce(a.contractnumber,'') = coalesce(b.contractnumber,'')
        and a.original_cycle_date = b.original_cycle_date
        and a.original_batch_id = b.original_batch_id
),
force_error_valid as (
    select distinct original_cycle_date,
        original_batch_id
    from final
    where len(date_error_message_) > 0
        and original_cycle_date = '{cycle_date}'
        and original_batch_id = '{batchid}'
)
select transaction_date,
    secondary_ledger_code,
    data_type,
    contractnumber,
    contractsourcesystemname,
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
                and len(hard_error_message_) = 0 then 'Forced Error. Date Errors Present in Other Headers;'
            else nvl(hard_error_message_,'')
        end
    ) as hard_error_message,
    soft_error_message_ as soft_error_message,
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
            and (
                cast('{cycle_date}' as date) != cast(a.original_cycle_date as date)
                or cast('{batchid}' as int) != cast(a.original_batch_id as int)
            )
            and len(error_message) = 0 then 'Good - Reprocessed Error'
        when a.original_cycle_date is not null
            and a.original_batch_id is not null
            and len(hard_error_message) > 0 then 'Hard Error'
        when a.original_cycle_date is not null
            and a.original_batch_id is not null
            and len(error_message) > 0 then 'Soft Error'
        when a.original_cycle_date is not null
            and a.original_batch_id is not null
            and len(error_message) = 0 then 'Good - Current Load'
        when a.original_cycle_date is not null
            and a.original_batch_id is not null
            and len(error_message) > 0 then 'Unknown Error'
        else 'Unknown'
    end as error_cleared,
    sblggr_include_exclude,
    ldgr_include_exclude
from final a
left join force_error_valid b using (
    original_cycle_date,
    original_batch_id
)
;
