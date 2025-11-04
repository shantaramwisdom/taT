select
	SERVICE_DETAIL_1.sd_amount,
	SERVICE_DETAIL_1.benefit_adjustment_id as ActivitySourceDisbursementIdentifier,
	SERVICE_DETAIL_1.sd_reported_flg,
	SERVICE_DETAIL_1.view_row_create_day as ActivityReportedDate,
	SERVICE_DETAIL_1.sd_benefit_adjustment_assoc_dt as ActivityEffectiveDate,
	SERVICE_DETAIL_1.payee_user_id as ActivitySourceOriginatingUserID,
	SERVICE_DETAIL_1.part_service_type_code as ActivitySourceTransactionCode,
	SERVICE_DETAIL_1.service_detail_id as ClaimBenefitLineIndicator,
	case
		when SERVICE_DETAIL_1.doc_id is null then concat('NULL', SERVICE_DETAIL_1.payment_request_id)
		else concat(concat(trim(SERVICE_DETAIL_1.doc_id), '-'), SERVICE_DETAIL_1.payment_request_id)
	end as ActivitySourceSystemBatchIdentifier,
	SERVICE_DETAIL_1.payment_request_id,
	concat(concat(POLICY_1.policy_no, '-'), SERVICE_DETAIL_1.rfb_id) as ActivitySourceClaimIdentifier,
	SERVICE_DETAIL_1.payment_request_detail_id,
	BENEFIT_ADJUSTMENT_1.payee_eft_flg,
	POLICY_1.policy_no as ContractNumber,
	SERVICE_DETAIL_1.policy_id,
	SERVICE_DETAIL_2.activityreversalcode,
	cast('${cycle_date}' as date) as cycle_date,
	case
		when SERVICE_DETAIL_2.sumamountsign = 'NEG' then cast(SERVICE_DETAIL_1.sd_amount as float) * -1
		else cast(SERVICE_DETAIL_1.sd_amount as float)
	end as activityamount,
	cast('${batchid}' as int) as batch_id
from
(
	select
		SERVICE_DETAIL.doc_id,
		SERVICE_DETAIL.sd_amount,
		SERVICE_DETAIL.benefit_adjustment_id,
		SERVICE_DETAIL.sd_reported_flg,
		SERVICE_DETAIL.view_row_create_day,
		SERVICE_DETAIL.sd_benefit_adjustment_assoc_dt,
		SERVICE_DETAIL.payee_user_id,
		SERVICE_DETAIL.cycle_date,
		SERVICE_DETAIL.part_service_type_code,
		SERVICE_DETAIL.service_detail_id,
		SERVICE_DETAIL.payment_request_id,
		SERVICE_DETAIL.rfb_id,
		SERVICE_DETAIL.payment_request_detail_id,
		SERVICE_DETAIL.policy_id
	from
		{source_database}.service_detail_effective_history_daily_{cycledate} as SERVICE_DETAIL
	where
		cast(Service_Detail.VIEW_ROW_CREATE_DAY as date) = '${cycle_date}'
		and '${cycle_date}' <= cast(Service_Detail.VIEW_ROW_OBSOLETE_DAY as date)
		and Service_Detail.BENEFIT_ADJUSTMENT_ID is not null
		and '${cycle_date}' = cast(SERVICE_DETAIL.sd_benefit_adjustment_assoc_dt as DATE)
) SERVICE_DETAIL_1
left join (
	select
		SERVICE_DETAIL.benefit_adjustment_id,
		case
			when Sum(cast(SERVICE_DETAIL.sd_amount as float)) < 0.0 then 'NEG'
			else 'POS'
		end as sumamountsign,
		case
			when Sum(cast(SERVICE_DETAIL.sd_amount as float)) < 0.0 then 'R'
			else 'O'
		end as activityreversalcode
	from
		{source_database}.service_detail_effective_history_daily_{cycledate} as SERVICE_DETAIL
	where
		cast(Service_Detail.VIEW_ROW_CREATE_DAY as date) = '${cycle_date}'
		and '${cycle_date}' <= cast(Service_Detail.VIEW_ROW_OBSOLETE_DAY as date)
		and Service_Detail.BENEFIT_ADJUSTMENT_ID is not null
		and cast(Service_Detail.VIEW_ROW_CREATE_DAY as date) = cast(SERVICE_DETAIL.sd_benefit_adjustment_assoc_dt as DATE)
	group by
		SERVICE_DETAIL.benefit_adjustment_id) SERVICE_DETAIL_2 on
	SERVICE_DETAIL_1.benefit_adjustment_id = SERVICE_DETAIL_2.benefit_adjustment_id
inner join (
	select
		BENEFIT_ADJUSTMENT.payee_eft_flg,
		BENEFIT_ADJUSTMENT.benefit_adjustment_id
	from
		{source_database}.benefit_adjustment_effective_history_daily_{cycledate} as BENEFIT_ADJUSTMENT
	where
		cast(Benefit_Adjustment.VIEW_ROW_CREATE_DAY as date) <= '${cycle_date}'
		and '${cycle_date}' <= cast(Benefit_Adjustment.VIEW_ROW_OBSOLETE_DAY as date)
		and Benefit_Adjustment.Benefit_Adjustment_ID is not null) BENEFIT_ADJUSTMENT_1 on
	SERVICE_DETAIL_1.benefit_adjustment_id = BENEFIT_ADJUSTMENT_1.benefit_adjustment_id
inner join (
	select
		policy_id,
		policy_no
	from
		{source_database}.policy_effective_history_daily_{cycledate} as POLICY
	where
		cast(Policy.VIEW_ROW_CREATE_DAY as date) <= '${cycle_date}'
		and '${cycle_date}' <= cast(Policy.VIEW_ROW_OBSOLETE_DAY as date)
		and cast(Policy.VIEW_ROW_EFFECTIVE_DAY as date) <= '${cycle_date}'
		and '${cycle_date}' <= cast(Policy.VIEW_ROW_EXPIRATION_DAY as date) ) POLICY_1 on
	SERVICE_DETAIL_1.policy_id = POLICY_1.policy_id
