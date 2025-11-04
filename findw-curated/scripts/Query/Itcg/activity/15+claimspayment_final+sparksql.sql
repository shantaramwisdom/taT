select
from_utc_timestamp(CURRENT_TIMESTAMP,'US/Central') AS recorded_timestamp
,'${source_system_name}' AS source_system_name
,'claimspayment' as activity_sourced_from_desc
,SHA2(concat(COALESCE(activityamounttype,''),COALESCE(activitytype,''),COALESCE(sourceactivityid,''),COALESCE('LTCG','')),256) as documentid,
SHA2(concat(COALESCE(contractnumber,''),COALESCE('LTCG','')),256) as fkcontractdocumentid,
SHA2(concat(COALESCE(activitysourceclaimidentifier,''),COALESCE('LTCG','')),256) as fkclaimdocumentid,
SHA2(concat(COALESCE(activitysourceclaimbenefitidentifier,''),COALESCE('LTCG','')),256) as fkclaimbenefitdocumentid,
activityamounttype,
activitytype,
sourceactivityid,
'LTCG' as sourcesystemname,
contractnumber,
contractadministrationlocationcode,
activityaccountingbalancedentryindicator,
generateSameUUID_uuids(concat(COALESCE('LTCG',''),COALESCE(activitytype,''),COALESCE(sourceactivityid,''),COALESCE(activitysourceactivityparentid,''),COALESCE(activityamounttype,''),COALESCE(cycle_date,''))) as activityaccountingid,
activityamount,
null as activitydepositsourcebatchidentifier,
activityeffectivedate,
activitygeneralledgerapplicationareacode,
activitygeneralledgersourcecode,
amt.ACT_MNY_MTHD_DESC as activitymoneymethod,
activityreporteddate,
activityreversalcode,
'XX' as activitysourceaccountingdeviator,
'XX' as activitysourceaccountingmemo,
null as activitysourcedepositidentifier,
activitysourcedisbursementidentifier,
null as activitysourceparentsuspenseidentifier,
activitysourcesuspensereferencenumber,
'XX' as activitysourcesuspensereason,
null as activitysourcesystemaccountinstruction,
null as activitysourcesystemcenterinstruction,
activitysourcetransactioncode,
ActivityTypeGroup,
claimbenefitlineindicator,
null as fundclassindicator,
null as fundnumber,
activitylegalcompanycode,
activitysourceactivityparentid,
activitysourceclaimidentifier,
null as fundsourcefundidentifier,
activitysourcelegalentitycode,
activitysourcesystemactivityid,
ActivityEffectiveDate as activitytaxreportingeffectivedate,
null as activitypaymentduedate,
cycle_date,
batch_id
from
(
select
src.*,
case
when m2.actvty_amt_typ is null then '0'
else m2.actvty_amt_typ
end as ActivityAmountType,
case
when m2.actvty_typ is null then '0'
else m2.actvty_typ
end as ActivityType,
concat_ws('_',src.policy_id,src.payment_request_id,src.ActivitySourceSystemActivityID) as SourceActivityID,
case
when m2.ACTVTY_TYP_GRP is null then '0'
else m2.ACTVTY_TYP_GRP
end as ActivityTypeGroup,
ACTVTY_GL.ACTVTY_GL_APP_AREA_CD as activitygeneralledgerapplicationareacode,
ACTVTY_GL.ACTVTY_GL_SRC_CD as activitygeneralledgersourcecode,
'-' as activitymoneymethod,
trans.SRC_LGL_ENTY_CD as activitysourcelegalentitycode,
enty.LGL_ENTY_CD as activitylegalcompanycode,
coalesce(LOC_CD.CNTRT_ADMN_LOC_CD,'0') as contractadministrationlocationcode,
LOC_CD.CNTRT_ADMN_LOC_CD,
case when src.SRC_ACT_MNY_MTHD = 'NA' and enty.LGL_ENTY_CD = '07' then '5'
when src.SRC_ACT_MNY_MTHD = 'NA' and enty.LGL_ENTY_CD = '10' then '6'
else src.SRC_ACT_MNY_MTHD end as new_act_mny_mthd
from
(
select
claimspayment_src.*,
concat_ws('_',claimspayment_src.policy_id,claimspayment_src.payment_request_id) as ActivitySourceActivityParentID,
concat_ws('_',claimspayment_src.PAYMENT_REQUEST_DETAIL_ID,claimspayment_src.ClaimBenefitLineIndicator) as ActivitySourceSystemActivityID,
'B' as activityaccountingbalancedentryindicator,
'B' as activityreversalcode,
case
when claimspayment_src.BP_STATUS_CD in ('4','5','6') then 'VOID'
when claimspayment_src.BP_STATUS_CD = '7' then 'ESCH'
else claimspayment_src.PART_SERVICE_TYPE_CODE
end as activitysourcetransactioncode,
case
when claimspayment_src.PAYEE_CONFIGURATION_BP_DETAILS_CD = '1'
and claimspayment_src.BP_STATUS_CD = '1' then '3'
when claimspayment_src.PAYEE_CONFIGURATION_BP_DETAILS_CD = '1'
and claimspayment_src.BP_STATUS_CD in ('4','5','6')
and left(claimspayment_src.ActivityCheckNumber,2) = 'E' then '4'
when claimspayment_src.PAYEE_CONFIGURATION_BP_DETAILS_CD = '1'
and claimspayment_src.BP_STATUS_CD in ('4','5','6')
and left(claimspayment_src.ActivityCheckNumber,2) != 'E' then 'NA'
when claimspayment_src.PAYEE_CONFIGURATION_BP_DETAILS_CD = '2' then claimspayment_src.BP_EFT_FLG
end as SRC_ACT_MNY_MTHD
from
claimspayment_src) src
left join lkp_actvty_typ_typgrp m2 on
src.activitysourcetransactioncode = m2.actvty_src_trn_cd
and m2.ACTVTY_SRC_LGL_ENTY = '-'
and m2.ACTVTY_SNGL_PRM_IND = '-'
and m2.ACTVTY_ACCTNG_MEMO_CD = '-'
and m2.ACTVTY_FRST_YR_RNWL_IND = '-'
and m2.ACTVTY_REV_CD = '-'
and m2.ACTSRSCSYS_ACG_BAL_ENT_IND = 'B'
left join LKP_SRCSYSMNT lkp_src_sys_nm on
lkp_src_sys_nm.CNTRT_SRC_SYS_NM_SRC = 'LTCG'
left join LKP_STAT_CMPY_TRNSLATN trans on
trans.CNTRT_SRC_SYS_NM = 'LTCG'
and substr(src.ContractNumber,0,2) = trans.ORIGINTG_LGL_ENTY
LEFT JOIN LKP_LGL_ENTY_CD enty ON
trans.CNTRT_SRC_SYS_NM = 'LTCG'
and enty.SRC_LGL_ENTY_CD = trans.SRC_LGL_ENTY
AND enty.EFF_STOP_DT >= cycle_date
AND enty.EFF_STRT_DT <= cycle_date
left join lkp_cntrt_admn_loc_cd LOC_CD on
LOC_CD.CNTRT_SRC_SYS_NM_SRC = coalesce (lkp_src_sys_nm.CNTRT_SRC_SYS_NM,'@')
left join lkp_actvty_gl ACTVTY_GL on
ACTVTY_GL.ACTVTY_SRC_SYS_NM = 'LTCG')otr
left join lkp_amt_method amt on
amt.SRC_ACT_MNY_MTHD=otr.new_act_mny_mthd