select from_utc_timestamp(current_timestamp, 'US/Central') as recorded_timestamp,
       source_system_name,
       case
           when original_cycle_date = '{cycle_date}' and original_batch_id = {batchid}
                then null
           else original_cycle_date
       end as original_cycle_date,
       case
           when original_cycle_date = '{cycle_date}' and original_batch_id = {batchid}
                then null
           else original_batch_id
       end as original_batch_id,
       activity_accounting_id,
       transaction_number,
       line_number,
       contractnumber,
       contractsourcesystemname,
       default_amount_drvd as default_amount,
       debit_credit_indicator,
       ifrs17reportingcashflowtype,
       orig_gl_company,
       orig_gl_account,
       orig_gl_center,
       statutoryresidentcountrycode,
       statutoryresidentstatecode_drvd as statutoryresidentstatecode,
       cast('{cycle_date}' as date) as cycle_date,
       cast('{batchid}' as int) as batch_id
from valid_records
