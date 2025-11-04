select from_utc_timestamp(current_timestamp, 'US/Central') as recorded_timestamp,
       case
           when ('cycle_date' = original_cycle_date and 'batchid' = original_batch_id)
                then from_utc_timestamp(current_timestamp, 'US/Central')
           else cast(original_recorded_timestamp as timestamp)
       end as original_recorded_timestamp,
       cast(original_cycle_date as date) as original_cycle_date,
       cast(original_batch_id as int) as original_batch_id,
       'Hard' as error_classification_name,
       error_message,
       datediff('{cycle_date}', original_cycle_date) as error_record_aging_days,
       to_json(
           struct(
               secondary_ledger_code,
               transaction_date,
               data_type,
               contractnumber,
               contractsourcesystemname,
               default_amount,
               debit_credit_indicator,
               orig_gl_company,
               orig_gl_account,
               orig_gl_center,
               statutoryresidentstatecode,
               ifrs17reportingcashflowtype,
               random_counter,
               to_json(
                   struct(
                       transaction_number,
                       source_system_nm_drvd,
                       ledger_name_drvd,
                       event_type_code_drvd,
                       subledger_short_name_drvd,
                       transaction_date_drvd,
                       secondary_ledger_code_drvd,
                       default_amount_drvd,
                       statutoryresidentstatecode_drvd,
                       statutoryresidentcountrycode as statutoryresidentcountrycode_drvd
                   )
               )
           )
       ) as drvd_data,
       {curated_table_name}: as table_name,
       'N' as reprocess_flag
from source_df
where len(hard_error_message) > 0

union all

select from_utc_timestamp(current_timestamp, 'US/Central') as recorded_timestamp,
       case
           when ('cycle_date' = original_cycle_date and 'batchid' = original_batch_id)
                then from_utc_timestamp(current_timestamp, 'US/Central')
           else cast(original_recorded_timestamp as timestamp)
       end as original_recorded_timestamp,
       cast(original_cycle_date as date) as original_cycle_date,
       cast(original_batch_id as int) as original_batch_id,
       'Cleared' as error_classification_name,
       null as error_message,
       0 as error_record_aging_days,
       to_json(
           struct(
               secondary_ledger_code,
               transaction_date,
               data_type,
               contractnumber,
               contractsourcesystemname,
               default_amount,
               debit_credit_indicator,
               orig_gl_company,
               orig_gl_account,
               orig_gl_center,
               statutoryresidentstatecode,
               ifrs17reportingcashflowtype,
               random_counter,
               to_json(
                   struct(
                       transaction_number,
                       activity_accounting_id,
                       line_number,
                       source_system_nm_drvd,
                       ledger_name_drvd,
                       event_type_code_drvd,
                       subledger_short_name_drvd,
                       transaction_date_drvd,
                       secondary_ledger_code_drvd,
                       default_amount_drvd,
                       statutoryresidentstatecode_drvd,
                       statutoryresidentcountrycode as statutoryresidentcountrycode_drvd
                   )
               )
           )
       ) as drvd_data,
       cast('{cycle_date}' as date) as cycle_date,
       cast('{batchid}' as int) as batch_id,
       old_error_message
) as error_record,
{curated_table_name}: as table_name,
'R' as reprocess_flag
from valid_records a
where original_cycle_date is not null
  and original_batch_id is not null
  and (original_cycle_date != '{cycle_date}' or original_batch_id != '{batchid}')
