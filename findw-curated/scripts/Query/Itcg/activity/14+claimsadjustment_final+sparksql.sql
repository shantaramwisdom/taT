select
from_utc_timestamp(CURRENT_TIMESTAMP,'US/Central') AS recorded_timestamp
,'${source_system_name}' AS source_system_name
,'claimsadjustment' as activity_sourced_from_desc
,SHA2(concat(COALESCE(activityamounttype,''),COALESCE(activitytype,''),COALESCE(sourceactivityid,''),COALESCE('LTCG','')),256) as documentid,
SHA2(concat(COALESCE(contractnumber,''),COALESCE('LTCG','')),256) as fkcontractdocumentid,
SHA2(concat(COALESCE(activitysourceclaimbenefitidentifier,''),COALESCE('LTCG','')),256) as fkclaimbenefitdocumentid,
SHA2(concat(COALESCE(activitysourceclaimidentifier,''),COALESCE('LTCG','')),256) as fkclaimdocumentid,
'LTCG' as sourcesystemname,
ActivityAmountType,
ActivityType,
sourceactivityid,
activityamount,
activityeffectivedate,
activityreporteddate,
null as activitysourcedepositidentifier,
activitysourcedisbursementidentifier,
null as activitysourceparentsuspenseidentifier,
activitysourcesuspensereferencenumber,
'XX' as activitysourceaccountingdeviator,
'XX' as activitysourceaccountingmemo,
activitysourceoriginatinguserid,
'XX' as activitysourcesuspensereason,
null as activitysourcesystemcenterinstruction,
claimbenefitlineindicator,
null as fundclassindicator,
null as fundnumber,
activitysourceclaimidentifier,
null as fundsourcefundidentifier,
activityreversalcode,
activitysourceactivityparentid,
'B' as activityaccountingbalancedentryindicator,
activitysourcetransactioncode,
activitygeneralledgerapplicationareacode,
activitygeneralledgersourcecode,
contractnumber,
null as activitychecknumber,
null as activitydepositsourcebatchidentifier,
activitymoneymethod,
ActivityTypeGroup,
activitysourcelegalentitycode,
activitylegalcompanycode,
generateSameUUID_uuids(concat(COALESCE('LTCG',''),COALESCE(activitytype,''),COALESCE(sourceactivityid,''),COALESCE(activitysourceactivityparentid,''),COALESCE(activityamounttype,''),COALESCE(cycle_date,''))) as activityaccountingid,
contractadministrationlocationcode,
cycle_date,
batch_id,
ActivityEffectiveDate as activitytaxreportingeffectivedate,
null as activitypaymentduedate
from
(
select
src.*,
Concat(src.payment_request_detail_id,src.claimbenefitlineindicator) as activitysourcesystemactivityid,
Concat(src.policy_id,src.payment_request_id,src.payment_request_detail_id,src.claimbenefitlineindicator) as activitysourceactivityparentid,
concat(src.policy_id,src.payment_request_id,src.payment_request_detail_id,src.claimbenefitlineindicator) as sourceactivityid,
case
when lkp_typ_grp.actvty_amt_typ is null then '0'
else lkp_typ_grp.actvty_amt_typ
end as ActivityAmountType,
case
when lkp_typ_grp.actvty_typ is null then '0'
else lkp_typ_grp.actvty_typ
end as ActivityType,
case
when c_activitysourcetransactioncode is null then '0'
else c_activitysourcetransactioncode
end as activitysourcetransactioncode,
ACTVTY_GL.ACTVTY_GL_APP_AREA_CD as activitygeneralledgerapplicationareacode,
ACTVTY_GL.ACTVTY_GL_SRC_CD as activitygeneralledgersourcecode,
'No ActivityMoneyMethod' as activitymoneymethod,
case
when lkp_typ_grp.ACTVTY_TYP_GRP is null then '0'
else lkp_typ_grp.ACTVTY_TYP_GRP
end as ActivityTypeGroup,
trans.SRC_LGL_ENTY_CD as activitysourcelegalentitycode,
trans.LGL_ENTY_CD as activitylegalcompanycode,
coalesce(LOC_CD.CNTRT_ADMN_LOC_CD,'0') as contractadministrationlocationcode
from
claimsadjustment_src src
left join LKP_SRCSYSMNT lkp_src_sys_nm on
lkp_src_sys_nm.CNTRT_SRC_SYS_NM_SRC = 'LTCG'
left join lkp_actvty_typ_typgrp lkp_typ_grp on
src.c_activitysourcetransactioncode = lkp_typ_grp.actvty_src_trn_cd
and src.activityreversalcode = lkp_typ_grp.actvty_rev_cd
and 'B' = lkp_typ_grp.actsrscsys_acg_bal_ent_ind
and lkp_typ_grp.ACTVTY_SNGL_PRM_IND = '-'
and lkp_typ_grp.ACTVTY_ACCTNG_MEMO_CD = '-'
and lkp_typ_grp.ACTVTY_FRST_YR_RNWL_IND = '-'
left join LKP_STAT_CMPY_TRNSLATN trans on
trans.CNTRT_SRC_SYS_NM = 'LTCG' and
substr(src.ContractNumber,0,2) = trans.ORIGINTG_LGL_ENTY
LEFT JOIN LKP_LGL_ENTY_CD enty on
enty.SRC_LGL_ENTY_CD = trans.SRC_LGL_ENTY
left join lkp_cntrt_admn_loc_cd LOC_CD on
LOC_CD.CNTRT_SRC_SYS_NM_SRC = coalesce (lkp_src_sys_nm.CNTRT_SRC_SYS_NM,'@')
and LOC_CD.ADMIN_PRCSS_LEG_ENTY_CD_SRC = '-'
left join lkp_actvty_gl ACTVTY_GL on
ACTVTY_GL.ACTVTY_SRC_SYS_NM = 'LTCG'
left join lkp_amt_method amt on
amt.ACT_SRC_SYS_NAME='LTCG'
and src.payee_eft_flg = amt.SRC_ACT_MNY_MTHD)otr
