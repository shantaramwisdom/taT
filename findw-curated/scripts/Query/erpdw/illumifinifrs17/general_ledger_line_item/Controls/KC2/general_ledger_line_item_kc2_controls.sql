with source_data as (
    select trim(journ_date) as transaction_date,
           'WH' as gl_application_area_code,
           '499' as gl_source_code,
           '01' as secondary_ledger_code,
           'Financial' as data_type,
           trim(policy) as contractnumber,
           trim(amount) as default_amount,
           case
               when trim(debit_credit) = 'D' then 'Debit'
               when trim(debit_credit) = 'C' then 'Credit'
               else null
               end as debit_credit_indicator,
           trim(source_company) as orig_gl_company,
           trim(product_fund_code) as orig_gl_account,
           trim(product_fund_code) as orig_gl_center,
           trim(account) as sourcesystemgeneralledgeraccountnumber
    from {source_database}.accounting_detail_current
    where cycle_date = {cycledate}
),

source_adj_data as (
    select a.*
    from source_data a
             left join lkp_actvty_gl_sblgdr_nm b on a.gl_application_area_code = b.ACTVTY_GL_APP_AREA_CD
        and a.gl_source_code = b.ACTVTY_GL_SRC_CD
             left join lkp_lgl_enty_cd c2 on c2.cntrct_src_sys_nm = 'Illumifin' and c2.src_lgl_enty_cd = a.orig_gl_company
             left join lkp_actvty_ldgr_nm c on '01' = c.actvty_gl_ldgr_cd
        and c2.lgl_enty_cd = c.actvty_lgl_enty_cd
    where c.include_exclude = 'Exclude'
       or a.orig_gl_company not in ('301', '501', '701')
)

select cast('{cycle_date}' as date) as cycle_date,
       cast({batchid} as int) batch_id,
       cast(
           cast(
                   nvl(trim(default_amount), 0) as numeric(18, 2)
               ) as decimal(18, 2)
           ) as default_amount,
       debit_credit_indicator,
       measure_type_code
from (
         select default_amount,
                debit_credit_indicator,
                'S' as measure_type_code
         from source_data
         union all
         select default_amount,
                debit_credit_indicator,
                'SA' as measure_type_code
         from source_adj_data
     ) a
