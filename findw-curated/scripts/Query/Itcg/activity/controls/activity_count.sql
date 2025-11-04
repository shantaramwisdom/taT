SELECT {batchid} batch_id,
       {cycle_date} cycle_date,
       {domain_name} domain,
       {source_system_name} sys_nm,
       {source_system_name} p_sys_nm,
       'BALANCING COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'integer' measure_value_datatype,
       'S' measure_src_tgt_adj_indicator,
       '{source_database}.gl_detail_effective_history_daily_{cycledate}/benefit_adjustment_effective_history_daily_{cycledate}/benefit_payment_effective_history_daily_{cycledate}' measure_table,
       SUM(measure_value) measure_value
FROM (
      with policy_effective_history as (
           select distinct POLICY.policy_id
           from {source_database}.Policy_effective_history_daily_{cycledate} POLICY
           left anti join ltcg_expired_contracts expired on policy.policy_no = expired.policy_no
           union all
           select ''
      )
      SELECT COUNT(*) measure_value
      FROM {source_database}.gl_detail_effective_history_daily_{cycledate} a
      inner join policy_effective_history POLICY on nvl(a.policy_id,'') = nvl(POLICY.policy_id,'')
      UNION ALL
      SELECT COUNT(*) measure_value
      FROM {source_database}.benefit_adjustment_effective_history_daily_{cycledate} AS BENEFIT_ADJUSTMENT
      FULL JOIN {source_database}.service_detail_effective_history_daily_{cycledate} AS SERVICE_DETAIL ON SERVICE_DETAIL.benefit_adjustment_id = BENEFIT_ADJUSTMENT.benefit_adjustment_id
      inner join policy_effective_history POLICY on nvl(SERVICE_DETAIL.policy_id,'') = nvl(POLICY.policy_id,'')
      UNION ALL
      SELECT COUNT(*) measure_value
      FROM {source_database}.benefit_payment_effective_history_daily_{cycledate} AS BENEFIT_PAYMENT
      JOIN {source_database}.service_detail_effective_history_daily_{cycledate} Service_DETAIL ON BENEFIT_PAYMENT.Benefit_Payment_ID = SERVICE_DETAIL.Benefit_Payment_ID
      inner join policy_effective_history POLICY on nvl(SERVICE_DETAIL.policy_id,'') = nvl(POLICY.policy_id,'')
)
UNION ALL
SELECT {batchid} batch_id,
       {cycle_date} cycle_date,
       {domain_name} domain,
       {source_system_name} sys_nm,
       {source_system_name} p_sys_nm,
       'BALANCING COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'integer' measure_value_datatype,
       'A' measure_src_tgt_adj_indicator,
       '{source_database}.gl_detail_effective_history_daily_{cycledate}/benefit_adjustment_effective_history_daily_{cycledate}/benefit_payment_effective_history_daily_{cycledate}' measure_table,
       SUM(measure_value) measure_value
FROM (
      with policy_effective_history as (
           select distinct POLICY.policy_id
           from {source_database}.Policy_effective_history_daily_{cycledate} POLICY
           left anti join ltcg_expired_contracts expired on policy.policy_no = expired.policy_no
           union all
           select ''
      ),
      --premium
      mapping_expr_activitypremium as (
           select GL_DETAIL_INLINE_QUERY.gl_transaction_type_desc,
                  GL_DETAIL_INLINE_QUERY.gl_transaction_type_cd,
                  GL_DETAIL_INLINE_QUERY.cycle_date,
                  GL_DETAIL_INLINE_QUERY.gl_transaction_amt,
                  GL_DETAIL_INLINE_QUERY.fgd_tran_dt,
                  GL_DETAIL_INLINE_QUERY.view_row_create_day,
                  GL_DETAIL_INLINE_QUERY.fgd_receipt_id,
                  GL_DETAIL_INLINE_QUERY.fgd_bill_account_id,
                  GL_DETAIL_INLINE_QUERY.fgd_receivable_id,
                  GL_DETAIL_INLINE_QUERY.fgd_adjustment_id,
                  GL_DETAIL_INLINE_QUERY.fga_account_cd,
                  GL_DETAIL_INLINE_QUERY.fgd_refund_id,
                  GL_DETAIL_INLINE_QUERY.policy_id,
                  GL_DETAIL_INLINE_QUERY.fgd_gl_detail_id,
                  GL_DETAIL_INLINE_QUERY.fgd_reference_id,
                  GL_DETAIL_INLINE_QUERY.fgs_source_cd,
                  GL_DETAIL_INLINE_QUERY.debitcreditindicator,
                  GL_DETAIL_INLINE_QUERY.concatenateid,
                  GL_DETAIL_COLLAPSE_LOGIC.transactiondeviator
           from (
                 select GL_DETAIL_TABLE.gl_transaction_type_desc,
                        GL_DETAIL_TABLE.gl_transaction_type_cd,
                        GL_DETAIL_TABLE.cycle_date,
                        GL_DETAIL_TABLE.gl_transaction_amt,
                        GL_DETAIL_TABLE.fgd_tran_dt,
                        GL_DETAIL_TABLE.view_row_create_day,
                        GL_DETAIL_TABLE.fgd_receipt_id,
                        GL_DETAIL_TABLE.fgd_bill_account_id,
                        GL_DETAIL_TABLE.fgd_receivable_id,
                        GL_DETAIL_TABLE.fgd_adjustment_id,
                        GL_DETAIL_TABLE.fga_account_cd,
                        GL_DETAIL_TABLE.fgd_refund_id,
                        GL_DETAIL_TABLE.policy_id,
                        GL_DETAIL_TABLE.fgd_gl_detail_id,
                        GL_DETAIL_TABLE.fgd_reference_id,
                        GL_DETAIL_TABLE.fgs_source_cd,
                        Concat(
                              Concat(
                              Concat(
                              Concat(
                              case when GL_DETAIL_TABLE.fgd_reference_id is null then ''
                                   else GL_DETAIL_TABLE.fgd_reference_id end,
                              case when GL_DETAIL_TABLE.fgs_source_cd is null then ''
                                   else GL_DETAIL_TABLE.fgs_source_cd end),
                              case when GL_DETAIL_TABLE.fgd_receivable_id is null then ''
                                   else GL_DETAIL_TABLE.fgd_receivable_id end),
                              case when GL_DETAIL_TABLE.fgd_adjustment_id is null then ''
                                   else GL_DETAIL_TABLE.fgd_adjustment_id end),
                              case when GL_DETAIL_TABLE.fgd_receipt_id is null then ''
                                   else GL_DETAIL_TABLE.fgd_receipt_id end),
                              case when GL_DETAIL_TABLE.fgd_refund_id is null then ''
                                   else GL_DETAIL_TABLE.fgd_refund_id end
                              ) as ConcatenateID,
                        case
                             when GL_DETAIL_TABLE.gl_transaction_type_cd = '1' then 'D'
                             when GL_DETAIL_TABLE.gl_transaction_type_cd = '2' then 'C'
                             when GL_DETAIL_TABLE.gl_transaction_type_cd = '0' then 'M'
                             else '@'
                        end as Debitcreditindicator
                 from {source_database}.gl_detail_effective_history_daily_{cycledate} as GL_DETAIL_TABLE
                 where GL_DETAIL_TABLE.gl_transaction_type_cd in ('1','2')
           ) GL_DETAIL_INLINE_QUERY
           inner join (
                 select concatenateid,
                        max(TransactionDeviator) TransactionDeviator
                 from (
                       select GL_DETAIL_TABLE.concatenateid,
                              array_join(
                              collect_list(
                              concat(
                              GL_DETAIL_TABLE.debitcreditindicator,
                              coalesce(GL_DETAIL_TABLE.fga_account_cd,''))
                              ) over (
                              partition by GL_DETAIL_TABLE.concatenateid
                              order by GL_DETAIL_TABLE.debitcreditindicator desc,
                              cast(GL_DETAIL_TABLE.fga_account_cd as int)
                              )
                             ) as TransactionDeviator
                       from (
                             select GL_DETAIL_TABLE.gl_transaction_type_cd,
                                    GL_DETAIL_TABLE.cycle_date,
                                    GL_DETAIL_TABLE.gl_transaction_amt,
                                    GL_DETAIL_TABLE.fgd_tran_dt,
                                    GL_DETAIL_TABLE.view_row_create_day,
                                    GL_DETAIL_TABLE.fgd_receipt_id,
                                    GL_DETAIL_TABLE.fgd_bill_account_id,
                                    GL_DETAIL_TABLE.fgd_receivable_id,
                                    GL_DETAIL_TABLE.fgd_adjustment_id,
                                    GL_DETAIL_TABLE.fga_account_cd,
                                    GL_DETAIL_TABLE.fgd_refund_id,
                                    GL_DETAIL_TABLE.policy_id,
                                    GL_DETAIL_TABLE.fgd_gl_detail_id,
                                    GL_DETAIL_TABLE.fgd_reference_id,
                                    GL_DETAIL_TABLE.fgs_source_cd,
                                    Concat(
                                        Concat(
                                        Concat(
                                        Concat(
                                        case
                                            when GL_DETAIL_TABLE.fgd_reference_id is null then ''
                                            else GL_DETAIL_TABLE.fgd_reference_id
                                        end,
                                        case
                                            when GL_DETAIL_TABLE.fgs_source_cd is null then ''
                                            else GL_DETAIL_TABLE.fgs_source_cd
                                        end
                                        ),
                                        case
                                            when GL_DETAIL_TABLE.fgd_receivable_id is null then ''
                                            else GL_DETAIL_TABLE.fgd_receivable_id
                                        end
                                        ),
                                        case
                                            when GL_DETAIL_TABLE.fgd_adjustment_id is null then ''
                                            else GL_DETAIL_TABLE.fgd_adjustment_id
                                        end
                                        ),
                                        case
                                            when GL_DETAIL_TABLE.fgd_receipt_id is null then ''
                                            else GL_DETAIL_TABLE.fgd_receipt_id
                                        end
                                        ),
                                        case
                                            when GL_DETAIL_TABLE.fgd_refund_id is null then ''
                                            else GL_DETAIL_TABLE.fgd_refund_id
                                        end
                                    ) as ConcatenateID,
                                    case
                                        when GL_DETAIL_TABLE.gl_transaction_type_cd = '1' then 'D'
                                        when GL_DETAIL_TABLE.gl_transaction_type_cd = '2' then 'C'
                                        when GL_DETAIL_TABLE.gl_transaction_type_cd = '0' then 'M'
                                        else '@'
                                    end as DebitCreditIndicator
                                    from {source_database}.gl_detail_effective_history_daily_{cycledate} as GL_DETAIL_TABLE
                                    where GL_DETAIL_TABLE.gl_transaction_type_cd in ('1','2')
                                    ) GL_DETAIL_INLINE_QUERY
                                    group by concatenateid
                                    ) GL_DETAIL_COLLAPSE_LOGIC on GL_DETAIL_INLINE_QUERY.concatenateid = GL_DETAIL_COLLAPSE_LOGIC.concatenateid
                                    ),
                                    mapping_expr_invoice as (
                                    select receivable_id,
                                        a.policy_id,
                                        covered_from_dt,
                                        covered_to_dt
                                    from {source_database}.invoice_effective_history_daily_{cycledate} a
                                    where (
                                        cast(view_row_create_day as date) <= '{cycle_date}'
                                        and '{cycle_date}' <= cast(view_row_obsolete_day as date)
                                    )
                                    group by receivable_id,
                                            a.policy_id,
                                            covered_from_dt,
                                            covered_to_dt
                                    ),
                                    mapping_expr_policy as (
                                    select original_effective_dt,
                                        a.policy_id
                                    from {source_database}.policy_effective_history_daily_{cycledate} as a
                                    where (
                                        cast(view_row_create_day as date) <= '{cycle_date}'
                                        and '{cycle_date}' <= cast(view_row_obsolete_day as date)
                                    )
                                    and (
                                        cast(view_row_effective_day as date) <= '{cycle_date}'
                                        and '{cycle_date}' < cast(view_row_expiration_day as date)
                                    )
                                    ),
                                    mapping_expr_activitypremium_adj as (
                                    select gl_detail.*,
                                        ROW_NUMBER() OVER(
                                                PARTITION BY ConcatenateID
                                                ORDER BY gl_transaction_type_cd
                                        ) as row_num,
                                    CASE
                                        WHEN cast(covered_from_dt as date) < (
                                            CASE
                                                WHEN lower(cast(original_effective_dt as date)) != 'null' THEN add_months(cast(original_effective_dt as date),12)
                                                ELSE NULL
                                            END
                                        )
                                        and (
                                            CASE
                                                WHEN lower(original_effective_dt) != 'null' THEN add_months(cast(original_effective_dt as date),12)
                                                ELSE NULL
                                            END
                                        ) < cast(covered_to_dt as date) THEN 1
                                        ELSE 0
                                    END as len
                                    from mapping_expr_activitypremium gl_detail
                                    LEFT JOIN mapping_expr_invoice INVOICE_VIEW ON gl_detail.fgd_receivable_id = INVOICE_VIEW.receivable_id
                                    LEFT JOIN mapping_expr_policy POLICY_VIEW ON gl_detail.policy_id = POLICY_VIEW.policy_id
                                    LEFT JOIN (
                                            select 1 as n
                                            union all
                                            select 2
                                    ) nums ON length(
                                    CASE
                                        WHEN cast(covered_from_dt as date) < (
                                            CASE
                                                WHEN lower(cast(original_effective_dt as date)) != 'null' THEN add_months(cast(original_effective_dt as date),12)
                                                ELSE NULL
                                            END
                                        )
                                        and (
                                            CASE
                                                WHEN lower(original_effective_dt) != 'null' THEN add_months(cast(original_effective_dt as date),12)
                                                ELSE NULL
                                            END
                                        ) < cast(covered_to_dt as date) THEN 'F-R'
                                        ELSE '@'
                                    END
                                    ) >= nums.n
                                    )
                                    select count(*) measure_value
                                    from {source_database}.gl_detail_effective_history_daily_{cycledate} a
                                    inner join policy_effective_history POLICY on nvl(a.policy_id,'') = nvl(POLICY.policy_id,'')
                                    where gl_transaction_type_cd not in ('1','2')
                                    UNION ALL
                                    select (int(sum(len) * 0.5) - 1) measure_value
                                    from mapping_expr_activitypremium_adj a
                                    inner join policy_effective_history POLICY on nvl(a.policy_id,'') = nvl(POLICY.policy_id,'')
                                    UNION ALL
                                    select count(*) measure_value
                                    from mapping_expr_activitypremium_adj gl_detail
                                    inner join policy_effective_history POLICY on nvl(gl_detail.policy_id,'') = nvl(POLICY.policy_id,'')
                                    where cast(gl_detail.view_row_create_day as date) != '{cycle_date}'
                                    or TransactionDeviator in (
                                        select source_val
                                        from lkp_src_txn_inclexcl
                                        where return_val = 'Exclude'
                                    )
                                    or (
                                        row_num != 1
                                        and len = 0
                                    )
                                    or (
                                        row_num > 2
                                        and len = 1
                                    )
                                    and transactiondeviator not like 'X13%'
                                    and transactiondeviator not like 'X15%'
                                    and transactiondeviator not like 'Z3%'
                                    and transactiondeviator not like 'X34%'
                                    and transactiondeviator not like 'X35%'
                                    and transactiondeviator not like 'X36%'
                                    and transactiondeviator not like 'X38%'
                                    AND transactiondeviator not like 'D6C7C7X'
                                    UNION ALL
--claimadjustment
SELECT COUNT(*) measure_value
FROM {source_database}.benefit_adjustment_effective_history_daily_{cycledate} AS BENEFIT_ADJUSTMENT
FULL JOIN {source_database}.service_detail_effective_history_daily_{cycledate} AS SERVICE_DETAIL ON SERVICE_DETAIL.benefit_adjustment_id = BENEFIT_ADJUSTMENT.benefit_adjustment_id
inner join policy_effective_history POLICY on nvl(SERVICE_DETAIL.policy_id,'') = nvl(POLICY.policy_id,'')
WHERE cast(Benefit_Adjustment.VIEW_ROW_CREATE_DAY AS date) = '{cycle_date}'
OR ('{cycle_date}' > cast(Benefit_Adjustment.VIEW_ROW_OBSOLETE_DAY AS date)
OR Benefit_Adjustment.Benefit_Adjustment_ID IS NULL
OR cast(SERVICE_DETAIL.VIEW_ROW_CREATE_DAY AS date) = '{cycle_date}'
OR ('{cycle_date}' > cast(SERVICE_DETAIL.VIEW_ROW_OBSOLETE_DAY AS date)
OR SERVICE_DETAIL.BENEFIT_ADJUSTMENT_ID IS NULL
OR ('{cycle_date}' != cast(SERVICE_DETAIL.sd_benefit_adjustment_assoc_dt AS DATE)
OR SERVICE_DETAIL.sd_benefit_adjustment_assoc_dt is null
))
UNION ALL
--claimspayment
select count(*) measure_value
from {source_database}.benefit_payment_effective_history_daily_{cycledate} as BENEFIT_PAYMENT
join {source_database}.service_detail_effective_history_daily_{cycledate} Service_detail on BENEFIT_PAYMENT.Benefit_Payment_ID = SERVICE_DETAIL.Benefit_Payment_ID
inner join policy_effective_history POLICY on nvl(SERVICE_DETAIL.policy_id,'') = nvl(POLICY.policy_id,'')
where cast(Service_Detail.VIEW_ROW_CREATE_DAY as date) = '{cycle_date}'
or ('{cycle_date}' > cast(Service_Detail.VIEW_ROW_OBSOLETE_DAY as date)
or cast(BENEFIT_PAYMENT.view_row_create_day as date) = '{cycle_date}'
or ('{cycle_date}' > cast(BENEFIT_PAYMENT.view_row_obsolete_day as date)
or (
     coalesce(
     BENEFIT_PAYMENT.PAYEE_CONFIGURATION_BP_DETAILS_CD,
     '') != '2'
     )
or (
     coalesce(BENEFIT_PAYMENT.bp_eft_flg,'') != '2'
     or coalesce(BENEFIT_PAYMENT.BP_STATUS_CD,'') not in ('3')
     or cast(BENEFIT_PAYMENT.bp_check_dt as date) != '{cycle_date}'
     or BENEFIT_PAYMENT.bp_check_dt is null
     )
and (
     coalesce(BENEFIT_PAYMENT.bp_eft_flg,'') != '2'
     or coalesce(BENEFIT_PAYMENT.BP_STATUS_CD,'') not in ('4','5')
     )
and (
     coalesce(BENEFIT_PAYMENT.bp_eft_flg,'') != '1'
     or coalesce(BENEFIT_PAYMENT.BP_STATUS_CD,'') not in ('2','6')
     )
and (
     coalesce(BENEFIT_PAYMENT.BP_STATUS_CD,'') != '7'
     )
and (
     coalesce(
     BENEFIT_PAYMENT.PAYEE_CONFIGURATION_BP_DETAILS_CD,
     '') != '1'
     or (
     coalesce(BENEFIT_PAYMENT.BP_STATUS_CD,'') not in ('1')
     or (
     cast(BENEFIT_PAYMENT.BP_CREATION_DT as date) != '{cycle_date}'
     or BENEFIT_PAYMENT.BP_CREATION_DT is null
     )
     and (
     coalesce(BENEFIT_PAYMENT.BP_STATUS_CD,'') not in ('4','5','6')
     )
     )
     )
     )
     )
                                                                        
