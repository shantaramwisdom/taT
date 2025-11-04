with input as(
    select transaction_currency_code,
           reporting_currency_code,
           reporting_currency_exchange_date,
           reporting_currency_exchange_rate_daily,
           reporting_currency_exchange_rate_ytd,
           date_format(to_date('${cycletdate}','yyyyMMdd'),'yyyy-MM-dd') as cycle_date
    from ${source_database}.currency_rates_current
    where cycle_date = '${cycledate}'
),
source_data as(
    select *,
           case
                when dayofweek(last_day(reporting_currency_exchange_date)) = 7 then date_add(last_day(reporting_currency_exchange_date),-1)
                when dayofweek(last_day(reporting_currency_exchange_date)) = 1 then date_add(last_day(reporting_currency_exchange_date),-2)
                else last_day(reporting_currency_exchange_date) end as last_calender_day
    from input
),
source as(
    select count(*) as src_cnt
    from source_data
),
adjust as(
    select *,
           row_number() over(
               partition by
                    transaction_currency_code,
                    reporting_currency_code,
                    reporting_currency_exchange_date
               order by null
           ) as fltr
    from source_data
),
source_adj as(
    select sum(c) as adj_cnt
    from(
        select count(*) c
        from adjust
        where fltr > 1
        union all
        select count(*) c
        from adjust
        where fltr = 1
        and dayofweek(reporting_currency_exchange_date) not between 2 and 6
    )
),
target as(
    select 2*count(*) as rcd_cnt
    from adjust
    where fltr = 1
    and last_calender_day = reporting_currency_exchange_date
)
select ${batchid} batch_id,
       '${cycledate}' cycle_date,
       '${domain_name}' domain,
       '${source_system_name}' sys_nm,
       '${source_system_name}' p_sys_nm,
       'BALANCING_COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'INTEGER' measure_value_datatype,
       'S' measure_src_tgt_adj_indicator,
       '${source_database}.currency_rates_current' measure_table,
       src_cnt measure_value
from source
union all
select ${batchid} batch_id,
       '${cycledate}' cycle_date,
       '${domain_name}' domain,
       '${source_system_name}' sys_nm,
       '${source_system_name}' p_sys_nm,
       'BALANCING_COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'INTEGER' measure_value_datatype,
       'A' measure_src_tgt_adj_indicator,
       '${source_database}.currency_rates_current' measure_table,
       adj_cnt measure_value
from source_adj
union all
select ${batchid} batch_id,
       '${cycledate}' cycle_date,
       '${domain_name}' domain,
       '${source_system_name}' sys_nm,
       '${source_system_name}' p_sys_nm,
       'BALANCING_COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'INTEGER' measure_value_datatype,
       'TA' measure_src_tgt_adj_indicator,
       '${source_database}.currency_rates_current' measure_table,
       rcd_cnt measure_value
from target