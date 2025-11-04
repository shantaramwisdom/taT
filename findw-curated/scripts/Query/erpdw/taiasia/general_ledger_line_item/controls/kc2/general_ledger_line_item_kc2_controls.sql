with source_data as (
    select trim(CIRCE802_POL_FILE_DATE) as transaction_date,
           trim(CIRCE802_pol_le) || trim(circe802_pol_lg) as secondary_ledger_code,
           'Financial' as data_type,
           'TAI ASIA' as source_system_nm,
           trim(CIRCE802_POL_REF_NUMBER) as contractnumber,
           trim(CIRCE802_POL_TRANS_AMOUNT) as default_amount,
           trim(CIRCE802_POL_LE) || trim(CIRCE802_POL_LG) as orig_gl_company,
           trim(CIRCE802_POL_GEAC_ACCT_NUM) || trim(CIRCE802_POL_LOB) as orig_gl_account,
           trim(CIRCE802_POL_GEAC_CENTER) as orig_gl_center,
           case
               when trim(CIRCE802_POL_TRANS_AMOUNT) > 0 then 'Debit'
               when trim(CIRCE802_POL_TRANS_AMOUNT) < 0 then 'Credit'
               else NULL
           end as debit_credit_indicator
    from   {source_database}.accounting_detail_current
    where  cycle_date = {cycledate}
),

source_adj_data as (
    select a.*
    from   source_data a
           left join lkp_actvty_gl_sbldgr_nm b
           on '~' = b.ACTVTY_GL_APP_AREA_CD
           and '~' = b.ACTVTY_GL_SRC_CD
           and 'TAI ASIA' = b.src_sys_nm_desc
           left join lkp_actvty_ldgr_nm c
           on substr(secondary_ledger_code, 3, 2) = c.actvty_gl_ldgr_cd
           and secondary_ledger_code = c.sec_ldgr_cd
    where  c.include_exclude = 'Exclude'
)

select cast({cycle_date} as date) as cycle_date,
       cast({batchid} as int) as batch_id,
       abs(nvl(cast(default_amount as decimal(18,2)), 0.0)) as default_amount,
       orig_gl_company,
       orig_gl_center,
       orig_gl_account,
       debit_credit_indicator,
       measure_type_code
from   (
           select default_amount,
                  orig_gl_company,
                  orig_gl_center,
                  orig_gl_account,
                  debit_credit_indicator,
                  'S' as measure_type_code
           from   source_data

           union all

           select default_amount,
                  orig_gl_company,
                  orig_gl_center,
                  orig_gl_account,
                  debit_credit_indicator,
                  'SA' as measure_type_code
           from   source_adj_data
       ) a;