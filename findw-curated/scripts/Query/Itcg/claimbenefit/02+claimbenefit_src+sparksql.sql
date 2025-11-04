with service_detail as (
select
    *
from
    (
    select
        RFB_ID as serv_rfb_id,
        Reported_Episode_Of_Benefit_ID as serv_Reported_Episode_Of_Benefit_ID
    from
        {source_database}.Service_Detail_effective_history_daily_{cycledate}
        left join batch_tracking bt on
            'LTCG' = bt.source_system_name
    where
        cast(View_Row_Create_Day as DATE) = '{cycle_date}'
        and cast(View_Row_Create_Day as DATE) <= '{cycle_date}'
        and cast(View_Row_Obsolete_Day as date) > '{cycle_date}'
        and cast(View_Row_Effective_Day as date) <= '{cycle_date}'
        and cast(View_Row_Expiration_Day as date) > '{cycle_date}'
        and SD_REPORTED_FLG = '1'
        and ((SD_OK_TO_PAY_FLG = '1'
        and Benefit_Payment_ID is not null
        and Benefit_Adjustment_ID is null
        and SD_EARLIEST_REPORT_DT > bt.min_cycle_date)
        or (SD_OK_TO_PAY_FLG = '1'
        and Benefit_Payment_ID is null
        and Benefit_Adjustment_ID is not null))
    union all (
    select
        nvl(srvdt_1.rfb_id, srvdt_2.rfb_id) serv_rfb_id,
        nvl(srvdt_1.Reported_Episode_Of_Benefit_ID, srvdt_2.Reported_Episode_Of_Benefit_ID) serv_Reported_Episode_Of_Benefit_ID
    from
        (
        select
            *
        from
            {source_database}.Service_Detail_effective_history_daily_{cycledate}
            left join batch_tracking bt on
                'LTCG' = bt.source_system_name
        where
            cast(View_Row_Create_Day as DATE) = '{cycle_date}'
            and cast(View_Row_Create_Day as DATE) <= '{cycle_date}'
            and cast(View_Row_Obsolete_Day as date) > '{cycle_date}'
            and cast(View_Row_Effective_Day as date) <= '{cycle_date}'
            and cast(View_Row_Expiration_Day as date) > '{cycle_date}'
            and SD_REPORTED_FLG = '1'
            and SD_OK_TO_PAY_FLG = '1'
            and SD_EARLIEST_REPORT_DT > bt.min_cycle_date
        ) srvdt_1
        left join (
        select
            Payment_Request_ID,
            Policy_ID,
            max(RFB_ID) as RFB_ID,
            max(Reported_Episode_Of_Benefit_ID) as Reported_Episode_Of_Benefit_ID
        from
            {source_database}.Service_Detail_effective_history_daily_{cycledate}
        where
            cast(View_Row_Create_Day as DATE) = '{cycle_date}'
            and cast(View_Row_Create_Day as DATE) <= '{cycle_date}'
            and cast(View_Row_Obsolete_Day as date) > '{cycle_date}'
            and cast(View_Row_Effective_Day as date) <= '{cycle_date}'
            and cast(View_Row_Expiration_Day as date) > '{cycle_date}'
            and SD_REPORTED_FLG = '1'
            and SD_OK_TO_PAY_FLG = '1'
        group by
            Payment_Request_ID,
            Policy_ID
        ) srvdt_2 on
            srvdt_1.POLICY_ID = srvdt_2.POLICY_ID
            and srvdt_1.Payment_Request_ID = srvdt_2.Payment_Request_ID)))
query1 as (
select
    srvdet.*,
    srvdet.serv_rfb_id as servcl_rfb_id,
    srvdet.payment_request_detail_id as paymentrequestlineidentifier,
    clmelg.EB_STATUS_CD,
    clmelg.EB_STATUS_RSN_CD,
    0 as uniq_rec
from
    service_detail srvdet
inner join (
    select
        *
    from
        {source_database}.claim_Eligibility_effective_history_daily_{cycledate}
    where
        cast(View_Row_Create_Day as DATE) <= '{cycle_date}'
        and '{cycle_date}' <= cast(View_Row_Obsolete_Day as date)
        and cast(View_Row_Effective_Day as date) <= '{cycle_date}'
        and '{cycle_date}' < cast(View_Row_Expiration_Day as date)
    ) clmelg on
        srvdet.Policy_ID = clmelg.Policy_ID
        and srvdet.serv_rfb_id = clmelg.RFB_ID
        and srvdet.serv_Reported_Episode_Of_Benefit_ID = clmelg.Episode_of_benefit_ID),
query2 as (
select
    srvdet.*,
    coalesce(srvdet.serv_rfb_id, clmelg.RFB_ID) as servcl_rfb_id,
    srvdet.payment_request_detail_id as paymentrequestlineidentifier,
    clmelg.EB_STATUS_CD,
    clmelg.EB_STATUS_RSN_CD,
    1 as uniq_rec
from
    service_detail srvdet
inner join (
    select
        *
    from
        (
        select
            *,
            row_number() over(partition by POLICY_ID
            order by
                Policy_ID,
                Claim_Incurred_Dt desc,
                rfb_id desc) as rnk
        from
            {source_database}.Claim_Eligibility_effective_history_daily_{cycledate}
        where
            cast(View_Row_Create_Day as DATE) <= '{cycle_date}'
            and '{cycle_date}' <= cast(View_Row_Obsolete_Day as date)
            and cast(View_Row_Effective_Day as date) <= '{cycle_date}'
            and '{cycle_date}' < cast(View_Row_Expiration_Day as date))
    where
        rnk = 1
    ) clmelg on
        srvdet.Policy_ID = clmelg.Policy_ID
        and clmelg.Claim_Incurred_Dt = srvdet.SD_Service_Start_Dt
        and srvdet.SD_Service_Start_Dt <= clmelg.Claim_Expiration_Dt),
query3 as (
select
    *
from
    (
    select
        srvdet.*,
        coalesce(srvdet.serv_rfb_id, clmelg.RFB_ID) as servcl_rfb_id,
        srvdet.payment_request_detail_id as paymentrequestlineidentifier,
        clmelg.EB_STATUS_CD,
        clmelg.EB_STATUS_RSN_CD,
        row_number() over(partition by srvdet.payment_request_id,
        srvdet.payment_request_detail_id,
        srvdet.service_detail_id
        order by
            clmelg.RFB_ID desc) as uniq_rec
    from
        service_detail srvdet
    inner join (
        select
            *
        from
            (
            select
                *,
                row_number() over(partition by POLICY_ID,
                RFB_ID,
                CLAIM_STATUS_CD
                order by
                    Policy_ID,
                    claim_Status_Cd_priority,
                    claim_incurred_dt desc,
                    rfb_id desc) as rnk
            from
                (
                select
                    *,
                    case
                        when Claim_Status_Cd = '3' then 1
                        when Claim_Status_Cd = '7' then 2
                        when Claim_Status_Cd = '8' then 3
                        when Claim_Status_Cd = '4' then 4
                        when Claim_Status_Cd = '2' then 5
                        when Claim_Status_Cd = '6' then 6
                                                when Claim_Status_Cd = '5' then 7
                        when Claim_Status_Cd = '1' then 8
                        else 9
                    end as Claim_Status_Cd_priority
                from
                    {source_database}.Claim_Eligibility_effective_history_daily_{cycledate}
                where
                    cast(View_Row_Create_Day as DATE) <= '{cycle_date}'
                    and '{cycle_date}' <= cast(View_Row_obsolete_Day as date)
                    and cast(View_Row_Effective_Day as date) <= '{cycle_date}'
                    and '{cycle_date}' < cast(View_Row_Expiration_Day as date))
            where
                rnk = 1) clmelg on
        srvdet.Policy_ID = clmelg.Policy_ID
where
    uniq_rec = 1),
query4 as (
select
    *,
    serv_rfb_id as servcl_rfb_id,
    payment_request_detail_id as paymentrequestlineidentifier,
    '$' as EB_STATUS_CD,
    '$' as EB_STATUS_RSN_CD,
    1 as uniq_rec
from
    service_detail ),
policy as (
select
    *
from
    {source_database}.Policy_effective_history_daily_{cycledate} pehc
where
    cast(pehc.View_Row_Create_Day as date) <= '{cycle_date}'
    and '{cycle_date}' <= cast(pehc.View_Row_obsolete_Day as date)
    and cast(pehc.View_Row_Effective_Day as date) <= '{cycle_date}'
    and '{cycle_date}' < cast(pehc.View_Row_Expiration_Day as date) ),
benefit_pay as (
select
    *
from
    {source_database}.benefit_payment_effective_history_daily_{cycledate} bpehc
where
    cast(bpehc.VIEW_ROW_CREATE_DAY as date) <= '{cycle_date}'
    and '{cycle_date}' <= cast(bpehc.VIEW_ROW_obsolete_Day as date)),
spectrum as (
select
    *
from
    query1
union (
select
    a.*
from
    query2 a
    left join query1 b on
        a.PAYMENT_REQUEST_ID = b.PAYMENT_REQUEST_ID
        and a.PAYMENT_REQUEST_DETAIL_ID = b.PAYMENT_REQUEST_DETAIL_ID
        and a.SERVICE_DETAIL_ID = b.SERVICE_DETAIL_ID
where
    b.PAYMENT_REQUEST_ID is null)
union (
select
    a.*
from
    query3 a
    left join query2 b on
        a.PAYMENT_REQUEST_ID = b.PAYMENT_REQUEST_ID
        and a.PAYMENT_REQUEST_DETAIL_ID = b.PAYMENT_REQUEST_DETAIL_ID
        and a.SERVICE_DETAIL_ID = b.SERVICE_DETAIL_ID
    left join query1 c on
        a.PAYMENT_REQUEST_ID = c.PAYMENT_REQUEST_ID
        and a.PAYMENT_REQUEST_DETAIL_ID = c.PAYMENT_REQUEST_DETAIL_ID
        and a.SERVICE_DETAIL_ID = c.SERVICE_DETAIL_ID
where
    b.PAYMENT_REQUEST_ID is null
    and c.PAYMENT_REQUEST_ID is null)
union (
select
    a.*
from
    query4 a
    left join query3 b on
        a.PAYMENT_REQUEST_ID = b.PAYMENT_REQUEST_ID
        and a.PAYMENT_REQUEST_DETAIL_ID = b.PAYMENT_REQUEST_DETAIL_ID
        and a.SERVICE_DETAIL_ID = b.SERVICE_DETAIL_ID
    left join query2 c on
        a.PAYMENT_REQUEST_ID = c.PAYMENT_REQUEST_ID
        and a.PAYMENT_REQUEST_DETAIL_ID = c.PAYMENT_REQUEST_DETAIL_ID
        and a.SERVICE_DETAIL_ID = c.SERVICE_DETAIL_ID
    left join query1 d on
        a.PAYMENT_REQUEST_ID = d.PAYMENT_REQUEST_ID
        and a.PAYMENT_REQUEST_DETAIL_ID = d.PAYMENT_REQUEST_DETAIL_ID
        and a.SERVICE_DETAIL_ID = d.SERVICE_DETAIL_ID
where
    b.PAYMENT_REQUEST_ID is null
    and c.PAYMENT_REQUEST_ID is null
    and d.PAYMENT_REQUEST_ID is null))
select
    case
        when spec.SD_OK_TO_PAY_FLG = '1' then spec.SD_AMOUNT
        else '0.00'
    end as ClaimBenefitAmount,
    spec.PAYREQ_RECEIVED_DT as ClaimBenefitClaimInvoiceReceivedDate,
    case
        when benefit_pay.PAYEE_CONFIGURATION_BP_DETAILS_CD = '1'
            and benefit_pay.BP_STATUS_CD = '1' then benefit_pay.BP_CREATION_DT
        else benefit_pay.BP_CHECK_DT
    end as ClaimBenefitClaimPaymentDate,
    cast(spec.SERVICE_DETAIL_ID as int) as ClaimBenefitLineIndicator,
    case
        when spec.PART_SERVICE_TYPE_PART_FLG = '1' then 'Y'
        when spec.PART_SERVICE_TYPE_PART_FLG = '2' then 'N'
        else '$'
    end as ClaimBenefitPartnershipExchangeIndicator,
    spec.SD_EARLIEST_REPORT_DT as ClaimBenefitProcessedDate,
    spec.SD_AMOUNT as ClaimBenefitRequestedAmount,
    spec.SD_SERVICE_END_DT as ClaimBenefitServiceEndDate,
    spec.SD_SERVICE_START_DT as ClaimBenefitServiceStartDate,
    spec.SD_Service_Units as ClaimBenefitServiceUnits,
    spec.PART_SERVICE_TYPE_CODE as ClaimBenefitsTypeCode,
    concat(concat(Policy.POLICY_NO, '_'), spec.servcl_rfb_id) as SourceclaimIdentifier,
    spec.servcl_rfb_id as claimbenefitrequestforbenefitidentifier,
    spec.EB_STATUS_CD,
    spec.EB_STATUS_RSN_CD,
    Policy.POLICY_NO as ContractNumber,
    spec.SUPPLEMENTARY_STMT_CD,
    spec.SD_OK_REASON_DECISION_TYPE_CD,
    spec.SD_OK_REASON_CD,
    spec.paymentrequestlineidentifier,
    '{cycle_date}' as load_date
from
    spectrum spec
    inner join policy on
        spec.POLICY_ID = policy.POLICY_ID
    left join benefit_pay on
        spec.benefit_payment_id = benefit_pay.benefit_payment_id

