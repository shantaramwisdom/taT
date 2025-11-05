select from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as recorded_timestamp,
       source_system_name,
       case
           when original_cycle_date = '{cycle_date}'
               and original_batch_id = {batchid} then null
           else original_cycle_date
           end as original_cycle_date,
       case
           when original_cycle_date = '{cycle_date}'
               and original_batch_id = {batchid} then null
           else original_batch_id
           end as original_batch_id,
       activity_accounting_id,
       transaction_number,
       line_number,
       orig_gl_company as sourcelegalentitycode,
       contractnumber,
       contractsourcesystemname,
       default_amount_drvd as default_amount,
       debit_credit_indicator,
       orig_gl_account_drvd as orig_gl_account,
       orig_gl_center_drvd as orig_gl_center,
       orig_gl_company_drvd as orig_gl_company,
       ifrs17cohort,
       ifrs17grouping,
       ifrs17measurementmodel,
       ifrs17portfolio,
       ifrs17profitability,
       ifrs17reportingcashflowtype,
       statutoryresidentcountrycode,
       statutoryresidentstatecode_drvd as statutoryresidentstatecode,
       activitysourcectransactioncode,
       sourcesystemactivitydescription,
       sourcesystemgeneralledgeraccountnumber,
       plancode,
       checknumber,
       cast('{cycle_date}' as date) as cycle_date,
       cast({batchid} as int) as batch_id
from valid_records
