with source as (
    select transaction_currency_code,
           reporting_currency_code,
           reporting_currency_exchange_date,
           reporting_currency_exchange_rate_daily,
           reporting_currency_exchange_rate_ytd,
           date_format(to_date(${cycletodt},'yyyyMMdd'),'yyyy-MM-dd') as cycle_date
    from ${source_database}.currency_rates_current
    where cycle_date = ${cycledate}
    group by 1, 2, 3, 4, 5, 6
),

source_data as (
    select *,
           case
                when dayofweek(last_day(reporting_currency_exchange_date)) = 7 then date_add(last_day(reporting_currency_exchange_date),-1)
                when dayofweek(last_day(reporting_currency_exchange_date)) = 1 then date_add(last_day(reporting_currency_exchange_date),-2)
                else last_day(reporting_currency_exchange_date)
           end as last_calender_day
    from source
),

master as (
    select transaction_currency_code as generalledgertransactioncurrencycode,
           reporting_currency_code as generalledgerreportingcurrencycode,
           reporting_currency_exchange_date as generalledgerreportingcurrencyexchangeeffectiveperiodstartdate,
           dateadd(day,-1,(dateadd(year,1,reporting_currency_exchange_date))) as generalledgerreportingcurrencyexchangeeffectiveperiodstopdate,
           nvl(
               cast(
                   cast(
                       nvl(trim(reporting_currency_exchange_rate_daily),0) as numeric(18,6)
                   ) as decimal(18,6)
               ),
           0) as generalledgerreportingcurrencyexchangerate,
           'Daily Rate' as generalledgerreportingcurrencyconversionratetype
    from source_data
    where dayofweek(reporting_currency_exchange_date) between 2 and 6

    union all

    select transaction_currency_code as generalledgertransactioncurrencycode,
           reporting_currency_code as generalledgerreportingcurrencycode,
           last_day(reporting_currency_exchange_date) as generalledgerreportingcurrencyexchangeeffectiveperiodstartdate,
           last_day(reporting_currency_exchange_date) as generalledgerreportingcurrencyexchangeeffectiveperiodstopdate,
           nvl(
               cast(
                   cast(
                       nvl(trim(reporting_currency_exchange_rate_daily),0) as numeric(18,6)
                   ) as decimal(18,6)
               ),
           0) as generalledgerreportingcurrencyexchangerate,
           'Period End Rate' as generalledgerreportingcurrencyconversionratetype
    from source_data
    where last_calender_day = reporting_currency_exchange_date

    union all

    select transaction_currency_code as generalledgertransactioncurrencycode,
           reporting_currency_code as generalledgerreportingcurrencycode,
           last_day(reporting_currency_exchange_date) as generalledgerreportingcurrencyexchangeeffectiveperiodstartdate,
           last_day(reporting_currency_exchange_date) as generalledgerreportingcurrencyexchangeeffectiveperiodstopdate,
           nvl(
               cast(
                   cast(
                       nvl(trim(reporting_currency_exchange_rate_ytd),0) as numeric(18,6)
                   ) as decimal(18,6)
               ),
           0) as generalledgerreportingcurrencyexchangerate,
           'YTD Average Rate' as generalledgerreportingcurrencyconversionratetype
    from source_data
    where last_calender_day = reporting_currency_exchange_date
)

select from_utc_timestamp(CURRENT_TIMESTAMP,'US/Central') as recorded_timestamp,
       source_system_name,
       '${source_system_name}' as sourcesystemname,
       SHA2(concat(source_system_name,':',COALESCE(generalledgertransactioncurrencycode,''),':',COALESCE(generalledgerreportingcurrencycode,''),':',COALESCE(generalledgerreportingcurrencyexchangeeffectiveperiodstartdate,''),':',COALESCE(generalledgerreportingcurrencyconversionratetype,'')),256) as documentid,
       generalledgertransactioncurrencycode,
       generalledgerreportingcurrencycode,
       generalledgerreportingcurrencyconversionratetype,
       cast(generalledgerreportingcurrencyexchangeeffectiveperiodstartdate as date),
       cast(generalledgerreportingcurrencyexchangeeffectiveperiodstopdate as date),
       generalledgerreportingcurrencyexchangerate,
       cast(cycle_date as date) as cycle_date,
       cast('${batchid}' as int) as batch_id
from (
    select '${source_system_name}' as source_system_name, * from master
)