select
	cast('{batchid}' as int) as batch_id,
	SERVICE_DETAIL.sd_amount as activityamount,
	BENEFIT_PAYMENT_VIEW.bp_check_no as activitychecknumber,
	case
		when BENEFIT_PAYMENT_VIEW.PAYEE_CONFIGURATION_BP_DETAILS_CD = '1'
		and BENEFIT_PAYMENT_VIEW.BP_STATUS_CD = '1' then BENEFIT_PAYMENT_VIEW.BP_CREATION_DT
		else BENEFIT_PAYMENT_VIEW.bp_check_dt
	end as activityeffectivedate,
	cast('${cycle_date}' as date) as cycle_date,
	BENEFIT_PAYMENT_VIEW.bp_eft_flg,
	BENEFIT_PAYMENT_VIEW.view_row_create_day as activityreporteddate,
	SERVICE_DETAIL.policy_id,
	case
		when SERVICE_DETAIL.doc_id is null then concat('NULL', SERVICE_DETAIL.payment_request_id)
		else concat(concat(trim(SERVICE_DETAIL.doc_id), '-'), SERVICE_DETAIL.payment_request_id)
	end as activitysourcesystembatchidentifier,
	SERVICE_DETAIL.payment_request_id,
	concat(concat(trim(POLICY.policy_no), '-'), SERVICE_DETAIL.rfb_id) as activitysourceclaimidentifier,
	BENEFIT_PAYMENT_VIEW.benefit_payment_id as activitysourcedisbursementidentifier,
	SERVICE_DETAIL.payee_user_id as activitysourceoriginatinguserid,
	SERVICE_DETAIL.payment_request_detail_id,
	SERVICE_DETAIL.service_detail_id as claimbenefitlineindicator,
	SERVICE_DETAIL.part_service_type_code,
	BENEFIT_PAYMENT_VIEW.bp_status_cd,
	BENEFIT_PAYMENT_VIEW.PAYEE_CONFIGURATION_BP_DETAILS_CD,
	trim(POLICY.policy_no) as contractnumber
from
(
	select
		BENEFIT_PAYMENT.bp_status_cd,
		BENEFIT_PAYMENT.benefit_payment_id,
		BENEFIT_PAYMENT.bp_check_no,
		BENEFIT_PAYMENT.BP_CREATION_DT,
		BENEFIT_PAYMENT.bp_check_dt,
		BENEFIT_PAYMENT.bp_eft_flg,
		BENEFIT_PAYMENT.view_row_create_day,
		BENEFIT_PAYMENT.cycle_date,
		BENEFIT_PAYMENT.PAYEE_CONFIGURATION_BP_DETAILS_CD
	from
		{source_database}.benefit_payment_effective_history_daily_{cycledate} as BENEFIT_PAYMENT
	where
		cast(BENEFIT_PAYMENT.view_row_create_day as date) = '${cycle_date}'
		and cast(BENEFIT_PAYMENT.view_effective_day as date) <= '${cycle_date}'
		and cast(BENEFIT_PAYMENT.view_row_obsolete_day as date) >= '${cycle_date}'
		and ((BENEFIT_PAYMENT.BP_EFT_FLG = '2'
		and BENEFIT_PAYMENT.PAYEE_CONFIGURATION_BP_DETAILS_CD = '2'
		and (BENEFIT_PAYMENT.BP_STATUS_CD = '2'
		or BENEFIT_PAYMENT.BP_STATUS_CD = '3')
		and cast(BENEFIT_PAYMENT.bp_check_dt as date) = '${cycle_date}')
		or (BENEFIT_PAYMENT.BP_EFT_FLG = '2'
		and BENEFIT_PAYMENT.BP_STATUS_CD in ('4', '5'))
		or (BENEFIT_PAYMENT.BP_EFT_FLG = '1'
		and BENEFIT_PAYMENT.BP_STATUS_CD in ('2', '6'))
		or (BENEFIT_PAYMENT.BP_STATUS_CD = '7')
		or ((BENEFIT_PAYMENT.BP_EFT_FLG = '1'
		and BENEFIT_PAYMENT.PAYEE_CONFIGURATION_BP_DETAILS_CD = '1')
		and (BENEFIT_PAYMENT.BP_STATUS_CD in ('1')
		and cast(BENEFIT_PAYMENT.BP_CREATION_DT as date) = '${cycle_date}')
		or (BENEFIT_PAYMENT.BP_STATUS_CD in ('4', '5', '6'))))) BENEFIT_PAYMENT_VIEW
inner join (
	select
		SERVICE_DETAIL.doc_id,
		SERVICE_DETAIL.sd_amount,
		SERVICE_DETAIL.policy_id,
		SERVICE_DETAIL.payment_request_id,
		SERVICE_DETAIL.rfb_id,
		SERVICE_DETAIL.payee_user_id,
		SERVICE_DETAIL.payment_request_detail_id,
		SERVICE_DETAIL.service_detail_id,
		SERVICE_DETAIL.part_service_type_code,
		SERVICE_DETAIL.benefit_payment_id,
		SERVICE_DETAIL.view_row_create_day,
		SERVICE_DETAIL.view_row_expiration_day,
		SERVICE_DETAIL.view_row_effective_day,
		SERVICE_DETAIL.view_row_obsolete_day,
		SERVICE_DETAIL.cycle_date
	from
		{source_database}.service_detail_effective_history_daily_{cycledate} Service_detail
	where
		cast(Service_Detail.VIEW_ROW_CREATE_DAY as date) <= '${cycle_date}'
		and '${cycle_date}' <= cast(Service_Detail.VIEW_ROW_OBSOLETE_DAY as date) ) SERVICE_DETAIL on
	BENEFIT_PAYMENT_VIEW.Benefit_Payment_ID = SERVICE_DETAIL.Benefit_Payment_ID
inner join (
	select
		policy_no,
		policy_id
	from
		{source_database}.policy_effective_history_daily_{cycledate}
	where
		cast(VIEW_ROW_CREATE_DAY as date) <= '${cycle_date}'
		and '${cycle_date}' <= cast(VIEW_ROW_OBSOLETE_DAY as date)
		and cast(VIEW_ROW_EFFECTIVE_DAY as date) <= '${cycle_date}'
		and '${cycle_date}' <= cast(VIEW_ROW_EXPIRATION_DAY as date) ) POLICY on
	SERVICE_DETAIL.policy_id = POLICY.policy_id
