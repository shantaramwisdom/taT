with source_data as (
    select 'Financial' as data_type,
           trim(cirec802_pol_lg) || trim(cirec802_pol_lg) as secondary_ledger_code,
           'TAI ASIA' as source_system_nm,
           trim(cirec802_pol_file_date) as transaction_date,
           trim(cirec802_pol_ref_number) as contractnumber,
           CAST('cycle_date' AS DATE) as cycle_date
    from {source_database}.accounting_detail_current
    where cycle_date = {cycledate}
    group by 1, 2, 3, 4, 5, 6
),

source_adj_data as (
    select a.*
    from source_data a
         left join lkp_actvty_gl_sblgdr_nm b
                on '--' = b.ACTIVTY_GL_APP_AREA_CD
               and '--' = b.ACTIVTY_GL_SRC_CD
               and 'TAI ASIA' = b.src_sys_nm_desc
         left join lkp_actvty_lgdr_nm c
                on substr(secondary_ledger_code, 3, 2) = c.actvty_gl_lgdr_cd
               and secondary_ledger_code = c.sec_lgdr_cd
    where c.include_exclude = 'Exclude'
),

target_data as (
    select python_date_format_checker(transaction_date, '%m/%d/%Y') as transaction_date_chk,
           secondary_ledger_code,
           transaction_date,
           data_type,
           contractnumber,
           cycle_date,
           secondary_ledger_code as secondary_ledger_code_drvd,
           c.oracle_fah_lgdr_nm as ledger_name_drvd,
           source_system_nm as source_system_nm_drvd,
           b.evnt_typ_cd as event_type_code_drvd,
           b.oracle_fah_sublgdr_nm as subledger_short_name_drvd
    from source_data a
         left join lkp_actvty_gl_sblgdr_nm b
                on '--' = b.ACTIVTY_GL_APP_AREA_CD
               and '--' = b.ACTIVTY_GL_SRC_CD
               and 'TAI ASIA' = b.src_sys_nm_desc
         left join lkp_actvty_lgdr_nm c
                on substr(secondary_ledger_code, 3, 2) = c.actvty_gl_lgdr_cd
               and secondary_ledger_code = c.sec_lgdr_cd
    where nvl(c.include_exclude, 'NULLVAL') in ('Include', 'NULLVAL')
),

target_force_ignore as (
    select distinct cycle_date
    from target_data a
    where transaction_date_chk = 'false'
),

target_ignore as (
    select transaction_date
    from (
        select a.*,
               row_number() over (
                   partition by transaction_date,
                                data_type,
                                contractnumber,
                                secondary_ledger_code_drvd,
                                ledger_name_drvd,
                                source_system_nm_drvd,
                                event_type_code_drvd,
                                subledger_short_name_drvd
                   order by null
               ) as fltr
        from target_data a
             left anti join target_force_ignore b using (cycle_date)
        where transaction_date_chk = 'true'
          and ledger_name_drvd is not null
          and source_system_nm_drvd is not null
          and event_type_code_drvd is not null
          and subledger_short_name_drvd is not null
          and secondary_ledger_code not like '% %'
          and len(secondary_ledger_code) >= 4
    ) as a
    where fltr > 1
),

source_ignore as (
    select transaction_date
    from (
        select a.*,
               row_number() over (
                   partition by secondary_ledger_code,
                                transaction_date,
                                data_type,
                                contractnumber,
                                secondary_ledger_code_drvd,
                                ledger_name_drvd,
                                source_system_nm_drvd,
                                event_type_code_drvd,
                                subledger_short_name_drvd
                   order by null
               ) as fltr
        from (
            select a.*
            from target_data a
            where transaction_date_chk = 'false'
               or ledger_name_drvd is null
               or source_system_nm_drvd is null
               or event_type_code_drvd is null
               or subledger_short_name_drvd is null
            union all
            select a.*
            from target_data a
                 left semi join target_force_ignore b using (cycle_date)
            where transaction_date_chk = 'true'
              and ledger_name_drvd is not null
              and source_system_nm_drvd is not null
              and event_type_code_drvd is not null
              and subledger_short_name_drvd is not null
              and cast(nvl(transaction_date, 0) as INTEGER) > 0
        ) a
    ) as a
    where fltr > 1
)

select cast('{cycle_date}' as date) as cycle_date,
       cast('{batchid}' as int) batch_id,
       cast(substr(transaction_date, -4) as numeric) as transaction_date,
       measure_type_code
from (
    select transaction_date,
           'S' as measure_type_code
    from source_data
    union all
    select transaction_date,
           'SA' as measure_type_code
    from source_adj_data
    union all
    select transaction_date,
           'TI' as measure_type_code
    from target_ignore
    union all
    select transaction_date,
           'SI' as measure_type_code
    from source_ignore
) a;