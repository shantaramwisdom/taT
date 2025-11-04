select from_utc_timestamp(current_timestamp(), 'US/Central') as recorded_timestamp,
case
    when '(cycle_date)' = original_cycle_date
     and '(batchid)' = original_batch_id then from_utc_timestamp(current_timestamp(), 'US/Central')
     else original_recorded_timestamp
end as original_recorded_timestamp,
original_cycle_date,
original_batch_id,
'Soft' as error_classification_name,
error_message,
datediff('(cycle_date)', original_cycle_date) as error_record_aging_days,
to_json(
    struct (
        gl_application_area_code,
        gl_source_code,
        transaction_date,
        data_type,
        secondary_ledger_code,
        contractnumber,
        to_json(
            struct (
                gl_source_code_drvd,
                secondary_ledger_code_drvd,
                transaction_date_drvd,
                event_type_code_drvd,
                ledger_name_drvd,
                source_system_drvd,
                subledger_short_name_drvd,
                transaction_number_drvd
            )
        ) as drvd_data
    )
) as error_record,
'{curated_table_name}' as table_name,
'N' reprocess_flag
from source_df
where len(error_message) > 0
and len(hard_error_message) = 0;