select
'LTCG' as sourcesystem,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.gl_transaction_type_cd,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.gl_transaction_amt as ActivityAmount,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgd_tran_dt as ActivityEffectiveDate,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.view_row_create_day as ActivityReportedDate,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgd_receipt_id as ActivitySourceDepositIdentifier,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgd_bill_account_id as ActivitySourceParentSuspenseIdentifier,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgd_receivable_id as ActivitySourceSuspenseReferenceNumber,
cast('${batchid}' as int) as batch_id,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgd_adjustment_id,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.transactiondeviator,
cast(GL_DETAIL_COLLAPSE_LOGIC_PARTION.fga_account_cd as integer) as fga_account_cd,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.refund_id as ActivitySourceDisbursementIdentifier,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.policy_id,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgd_gl_detail_id,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgd_reference_id as ActivitySourceSystemBatchIdentifier,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgs_source_cd as ActivitySourceSystemActivityID,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.debitcreditindicator,
GL_DETAIL_COLLAPSE_LOGIC_PARTION.concatenateid,
cast('${cycle date}' as date) as cycle_date,
null as ActivitySourceAccountingDeviator,
null as ActivitySourceAccountingMemoCode,
null as ActivitySourceOriginatingUserID,
null as ActivitySourceSuspenseMatchingReferenceNumber,
null as ActivitySourceSuspenseReason,
null as ActivitySourceSystemAccountInstruction,
null as ActivitySourceSystemCenterInstruction,
null as ClaimBenefitLineIndicator,
null as FundClassIndicator,
null as FundNumber,
null as ActivitySourceClaimIdentifier,
null as FundSourceFundIdentifier,
POLICY_VIEW.policy_id as jp_policy_id,
POLICY_VIEW.original_effective_dt,
INVOICE_VIEW.covered_from_dt,
INVOICE_VIEW.covered_to_dt,
CREDIT_RECEIPT.receipt_batch_id,
CREDIT_RECEIPT.old_batch_id,
CREDIT_REFUND.check_number,
CREDIT_RECEIPT.bank_account_name,
CREDIT_RECEIPT.receipt_id,
CREDIT_ADJ.adjustment_id,
CREDIT_REFUND.refund_id,
POLICY_VIEW.policy_no as contractnumber
from
(
select
GL_DETAIL_COLLAPSE_LOGIC_1.gl_transaction_type_cd,
GL_DETAIL_COLLAPSE_LOGIC_1.cycle_date,
GL_DETAIL_COLLAPSE_LOGIC_1.gl_transaction_amt,
GL_DETAIL_COLLAPSE_LOGIC_1.fgd_tran_dt,
GL_DETAIL_COLLAPSE_LOGIC_1.view_row_create_day,
GL_DETAIL_COLLAPSE_LOGIC_1.fgd_receipt_id,
GL_DETAIL_COLLAPSE_LOGIC_1.fgd_bill_account_id,
GL_DETAIL_COLLAPSE_LOGIC_1.fgd_receivable_id,
GL_DETAIL_COLLAPSE_LOGIC_1.fgd_adjustment_id,
GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator,
GL_DETAIL_COLLAPSE_LOGIC_1.fga_account_cd,
GL_DETAIL_COLLAPSE_LOGIC_1.fgd_refund_id,
GL_DETAIL_COLLAPSE_LOGIC_1.policy_id,
GL_DETAIL_COLLAPSE_LOGIC_1.fgd_gl_detail_id,
GL_DETAIL_COLLAPSE_LOGIC_1.fgd_reference_id,
GL_DETAIL_COLLAPSE_LOGIC_1.fgs_source_cd,
GL_DETAIL_COLLAPSE_LOGIC_1.debitcreditindicator,
GL_DETAIL_COLLAPSE_LOGIC_1.concatenateid
from
(
select
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.gl_transaction_type_cd,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.cycle_date,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.gl_transaction_amt,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fgd_tran_dt,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.view_row_create_day,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fgd_receipt_id,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fgd_bill_account_id,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fgd_receivable_id,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fgd_adjustment_id,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.transactiondeviator,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fga_account_cd,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fgd_refund_id,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.policy_id,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fgd_gl_detail_id,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fgd_reference_id,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fgs_source_cd,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.debitcreditindicator,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.concatenateid,
row_number() over(partition by GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.concatenateid
order by
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.policy_id desc,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fga_account_cd,
GL_DETAIL_COLLAPSE_LOGIC_SUMMARY_CONCAT_ID.fgd_bill_account_id) as ConcatID_RowNum
from
(
select
GL_DETAIL_INLINE_QUERY.gl_transaction_type_desc,
GL_DETAIL_INLINE_QUERY.gl_transaction_type_cd,
GL_DETAIL_INLINE_QUERY.cycle_date,
GL_DETAIL_INLINE_QUERY.gl_transaction_amt,
GL_DETAIL_INLINE_QUERY.fgd_tran_dt,
GL_DETAIL_INLINE_QUERY.view_row_create_day,
GL_DETAIL_INLINE_QUERY.fgd_receipt_id,
GL_DETAIL_INLINE_QUERY.fgd_bill_account_id,
GL_DETAIL_INLINE_QUERY.fgd_receivable_id,
GL_DETAIL_INLINE_QUERY.fgd_adjustment_id,
GL_DETAIL_INLINE_QUERY.fgd_refund_id,
GL_DETAIL_INLINE_QUERY.policy_id,
GL_DETAIL_INLINE_QUERY.fgd_gl_detail_id,
GL_DETAIL_INLINE_QUERY.fgd_reference_id,
GL_DETAIL_INLINE_QUERY.fgs_source_cd,
GL_DETAIL_INLINE_QUERY.debitcreditindicator,
GL_DETAIL_INLINE_QUERY.concatenateid,
GL_DETAIL_COLLAPSE_LOGIC.transactiondeviator
from
(
select
GL_DETAIL_TABLE.gl_transaction_type_desc,
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
Concat(Concat(Concat(Concat(Concat(
case
when GL_DETAIL_TABLE.fgd_reference_id is null then ''
else GL_DETAIL_TABLE.fgd_reference_id
end,
case
when GL_DETAIL_TABLE.fgs_source_cd is null then ''
else GL_DETAIL_TABLE.fgs_source_cd
end,
case
when GL_DETAIL_TABLE.fgd_receivable_id is null then ''
else GL_DETAIL_TABLE.fgd_receivable_id
end,
case
when GL_DETAIL_TABLE.fgd_adjustment_id is null then ''
else GL_DETAIL_TABLE.fgd_adjustment_id
end,
case
when GL_DETAIL_TABLE.fgd_receipt_id is null then ''
else GL_DETAIL_TABLE.fgd_receipt_id
end,
case
when GL_DETAIL_TABLE.fgd_refund_id is null then ''
else GL_DETAIL_TABLE.fgd_refund_id
end)) as ConcatenateID,
case
when GL_DETAIL_TABLE.gl_transaction_type_cd = '1' then 'D'
when GL_DETAIL_TABLE.gl_transaction_type_cd = '2' then 'C'
when GL_DETAIL_TABLE.gl_transaction_type_cd = '0' then 'M'
else '0'
end as DebitcreditIndicator
from
{source_database}.gl_detail_effective_history_daily (${cycledate}) as GL_DETAIL_TABLE
where
GL_DETAIL_TABLE.gl_transaction_type_cd != '0'
and cast(GL_DETAIL_TABLE.VIEW_ROW_CREATE_DAY as date) = '${cycle_date}') GL_DETAIL_INLINE_QUERY
inner join (
	select
		concatenateid,
		max(TransactionDeviator) TransactionDeviator
	from
	(
		select
			GL_DETAIL_TABLE.concatenateid,
			array_join(collect_list(concat(GL_DETAIL_TABLE.debitcreditindicator,
			coalesce(GL_DETAIL_TABLE.fga_account_cd,
			''))) over (partition by GL_DETAIL_TABLE.concatenateid
			order by
			GL_DETAIL_TABLE.debitcreditindicator desc,
			cast(GL_DETAIL_TABLE.fga_account_cd as int)),
			'') as TransactionDeviator
		from
		(
			select
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
				Concat(Concat(Concat(Concat(
				case
					when GL_DETAIL_TABLE.fgd_reference_id is null then ''
					else GL_DETAIL_TABLE.fgd_reference_id
				end,
				case
					when GL_DETAIL_TABLE.fgs_source_cd is null then ''
					else GL_DETAIL_TABLE.fgs_source_cd
				end,
				case
					when GL_DETAIL_TABLE.fgd_receivable_id is null then ''
					else GL_DETAIL_TABLE.fgd_receivable_id
				end,
				case
					when GL_DETAIL_TABLE.fgd_adjustment_id is null then ''
					else GL_DETAIL_TABLE.fgd_adjustment_id
				end,
				case
					when GL_DETAIL_TABLE.fgd_receipt_id is null then ''
					else GL_DETAIL_TABLE.fgd_receipt_id
				end,
				case
					when GL_DETAIL_TABLE.fgd_refund_id is null then ''
					else GL_DETAIL_TABLE.fgd_refund_id
				end)) as ConcatenateID,
				case
					when GL_DETAIL_TABLE.gl_transaction_type_cd = '1' then 'D'
					when GL_DETAIL_TABLE.gl_transaction_type_cd = '2' then 'C'
					when GL_DETAIL_TABLE.gl_transaction_type_cd = '0' then 'M'
					else '0'
				end as DebitcreditIndicator
			from
				{source_database}.gl_detail_effective_history_daily ({cycledate}) as GL_DETAIL_TABLE
			where
				GL_DETAIL_TABLE.gl_transaction_type_cd != '0'
				and cast(GL_DETAIL_TABLE.VIEW_ROW_CREATE_DAY as date) = '{cycle_date}'
		) GL_DETAIL_INLINE_QUERY
	) group by
		concatenateid) GL_DETAIL_COLLAPSE_LOGIC on
	(GL_DETAIL_INLINE_QUERY.concatenateid = GL_DETAIL_COLLAPSE_LOGIC.concatenateid) GL_DETAIL_COLLAPSE_LOGIC_1
where
	(GL_DETAIL_COLLAPSE_LOGIC_1.concatid_rownum = 1
	and (GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator not like '%13x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator not like '%15x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator not like '%33x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator not like '%35x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator not like '%36x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator not like '%38x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator not like '%D6G77X%'))
	or (GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator like '%13x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator like '%15x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator like '%33x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator like '%35x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator like '%36x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator like '%38x%'
	or GL_DETAIL_COLLAPSE_LOGIC_1.transactiondeviator like '%D6G77X%')) GL_DETAIL_COLLAPSE_LOGIC_PARTION
left join (
	select
		POLICY.original_effective_dt,
		POLICY.policy_id,
		POLICY.policy_no
	from
		{source_database}.policy_effective_history_daily_{cycledate} as POLICY
	where
		( cast(POLICY.view_row_create_day as date) <= '{cycle_date}'
		and '{cycle_date}' <= cast(POLICY.view_row_obsolete_day as date))
		and ( cast(POLICY.view_row_effective_day as date) <= '{cycle_date}'
		and '{cycle_date}' <= cast(POLICY.view_row_expiration_day as date) )) POLICY_VIEW on
	GL_DETAIL_COLLAPSE_LOGIC_PARTION.policy_id = POLICY_VIEW.policy_id
left join (
	select
		INVOICE.receivable_id,
		INVOICE.policy_id,
		INVOICE.covered_from_dt,
		INVOICE.covered_to_dt
	from
		{source_database}.invoice_effective_history_daily_{cycledate} as INVOICE
	where
		( cast(INVOICE.view_row_create_day as date) <= '{cycle_date}'
		and '{cycle_date}' <= cast(INVOICE.view_row_obsolete_day as date))
	group by
		INVOICE.receivable_id,
		INVOICE.policy_id,
		INVOICE.covered_from_dt,
		INVOICE.covered_to_dt)INVOICE_VIEW on
	GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgd_receivable_id = INVOICE_VIEW.receivable_id
left join (
	select
		CREDIT.receipt_batch_id,
		CREDIT.old_batch_id,
		CREDIT.check_number,
		CREDIT.bank_account_name,
		CREDIT.receipt_id
	from
		{source_database}.credit_effective_history_daily_{cycledate} as CREDIT
	where
		( cast(CREDIT.view_row_create_day as date) <= '{cycle_date}'
		and '{cycle_date}' <= cast(CREDIT.view_row_obsolete_day as date))
		and CREDIT.credit_type_cd = '1')CREDIT_RECEIPT on
	GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgd_receipt_id = CREDIT_RECEIPT.receipt_id
left join (
	select
		CREDIT.receipt_batch_id,
		CREDIT.old_batch_id,
		CREDIT.check_number,
		CREDIT.bank_account_name,
		CREDIT.refund_id
	from
		{source_database}.credit_effective_history_daily_{cycledate} as CREDIT
	where
		( cast(CREDIT.view_row_create_day as date) <= '{cycle_date}'
		and '{cycle_date}' <= cast(CREDIT.view_row_obsolete_day as date))
		and CREDIT.credit_type_cd = '2')CREDIT_REFUND on
	GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgd_refund_id = CREDIT_REFUND.refund_id
left join (
	select
		CREDIT.receipt_batch_id,
		CREDIT.old_batch_id,
		CREDIT.check_number,
		CREDIT.bank_account_name,
		CREDIT.adjustment_id
	from
		{source_database}.credit_effective_history_daily_{cycledate} as CREDIT
	where
		( cast(CREDIT.view_row_create_day as date) <= '{cycle_date}'
		and '{cycle_date}' <= cast(CREDIT.view_row_obsolete_day as date))
		and CREDIT.credit_type_cd = '3')CREDIT_ADJ on
	GL_DETAIL_COLLAPSE_LOGIC_PARTION.fgd_adjustment_id = CREDIT_ADJ.adjustment_id