SELECT
from_utc_timestamp(CURRENT_TIMESTAMP,'US/Central') AS recorded_timestamp
,'${source_system_name}' as source_system_name
,'activitypremium' as activity_sourced_from_desc
,SHA2(concat(COALESCE(activityamounttype,''),COALESCE(activitytype,''),COALESCE(sourceactivityid,''),COALESCE('LTCG','')),256) as documentid
,SHA2(concat(COALESCE(contractnumber,''),COALESCE('LTCG','')),256) as fkcontractdocumentid
,SHA2(concat(COALESCE(activitysourceclaimbenefitidentifier,''),COALESCE('LTCG','')),256) as fkclaimbenefitdocumentid
,SHA2(concat(COALESCE(activitysourceclaimidentifier,''),COALESCE('LTCG','')),256) as fkclaimdocumentid
,activityamounttype
,activitytype
,sourceactivityid
,'LTCG' as sourcesystemname
,contractnumber
,contractadministrationlocationcode
,activityaccountingbalancedentryindicator
,generateSameUUID_uuids(concat(COALESCE('LTCG',''),COALESCE(activitytype,''),COALESCE(sourceactivityid,''),COALESCE(activitysourceactivityparentid,''),COALESCE(activityamounttype,''),COALESCE(cycle_date,''))) as activityaccountingid
,new_activityamount as activityamount
,case
when ActivitySourceDepositIdentifier is not null and receipt_batch_id is null then old_batch_id
when ActivitySourceDepositIdentifier is not null and receipt_batch_id is not null then receipt_batch_id
else null end as activitydepositsourcebatchidentifier
,activityeffectivedate
,activitygeneralledgerapplicationareacode
,activitygeneralledgersourcecode
,activitymoneymethod
,activityreporteddate
,activityreversalcode
,activitysourceaccountingdeviator
,activitysourceaccountingmemo
,activitysourcedepositidentifier
,activitysourcedisbursementidentifier
,null as activitysourceoriginatinguserid
,activitysourceparentsuspenseidentifier
,activitysourcesuspensematchingreferencenumber
,'XX' as activitysourcesuspensereason
,activitysourcesuspensereferencenumber
,activitysourcesystemcenterinstruction
,activitysourcesystemtransaction
,activitytypegroup
,claimbenefitlineindicator
,fundclassindicator
,fundnumber
,activitylegalcompanycode
,activitysourceactivityparentid
,activitysourceididentifier
,fundsourcefundidentifier
,activitysourcelegalentitycode
,activitysourcesystemactivityid
,ActivitySourceSystemBatchIdentifier
,ActivityEffectiveDate as activitytaxreportingeffectivedate
,case when ActivitySourceSuspenseReferenceNumber is null then null else covered_from_dt end as activitypaymentduedate
,case when ActivitySourceDisbursementIdentifier is not null then check_number else null end as activitychecknumber
,cycle_date
,batch_id
FROM
(
select
src.*,
case
when lower(src.original_effective_dt) != 'null' then add_months(src.original_effective_dt,12)
else null
end as original_effective_dt_1yr,
case
when m5.actvty_amt_typ is null then '0'
else m5.actvty_amt_typ
end as ActivityAmountType,
case
when m5.actvty_typ is null then '0'
else m5.actvty_typ
end as ActivityType,
case
when m5.ACTVTY_TYP_GRP is null then '0'
else m5.ACTVTY_TYP_GRP
end as ActivityTypeGroup,
case
when c_activitysourcetransactioncode is null then '0'
else c_activitysourcetransactioncode
end as activitysourcetransactioncode,
case
when src.activitysourcesuspensereferencenumber is not null
and src.ACTVTY_FRST_YR_RNWL_IND = 'F'
and to_date(src.covered_from_dt) < (case
when lower(src.original_effective_dt) != 'null' then add_months(src.original_effective_dt,12)
else null
end)
and (case
when lower(src.original_effective_dt) != 'null' then add_months(src.original_effective_dt,12)
else null
end) <= to_date(src.covered_to_dt) then round(months_between(to_date(src.covered_from_dt),(case when lower(src.original_effective_dt) != 'null' then add_months(src.original_effective_dt,12) else null end)) / months_between(to_date(src.covered_from_dt),to_date(src.covered_to_dt)) * cast(src.activityamount as decimal(18,2)),2) as decimal(18,2))
when src.activitysourcesuspensereferencenumber is not null
and src.ACTVTY_FRST_YR_RNWL_IND = 'R'
and to_date(covered_from_dt) < (case
when lower(src.original_effective_dt) != 'null' then add_months(src.original_effective_dt,12)
else null
end)
and (case
when lower(src.original_effective_dt) != 'null' then add_months(src.original_effective_dt,12)
else null
end) < to_date(src.covered_to_dt) then cast(src.activityamount = round(months_between(to_date(src.covered_from_dt),(case when lower(src.original_effective_dt) != 'null' then add_months(src.original_effective_dt,12) else null end)) / months_between(to_date(src.covered_from_dt),to_date(src.covered_to_dt)) * cast(src.activityamount as decimal(18,2)),2) as decimal(18,2))
else cast(src.activityamount as decimal(18,2))
end as new_activityamount,
ACTVTY_GL.ACTVTY_GL_APP_AREA_CD as activitygeneralledgerapplicationareacode,
ACTVTY_GL.ACTVTY_GL_SRC_CD as activitygeneralledgersourcecode,
ant.ACT_MNY_MTHD_DESC as activitymoneymethod,
trans.SRC_LGL_ENTY_CD as activitysourcelegalentitycode,
enty.LGL_ENTY_CD as activitylegalcompanycode,
coalesce(LOC_CD.CNTRT_ADMN_LOC_CD,'e') as contractadministrationlocationcode,
LOC_CD.CNTRT_ADMN_LOC_CD
from
(
select
activitypremium_src.*,
case
when activitypremium_src.policy_id is null
or activitypremium_src.activitysourcesuspensereferencenumber is null then 'F'
when activitypremium_src.activitysourcesuspensereferencenumber is not null
and (case
when lower(original_effective_dt) != 'null' then add_months(original_effective_dt,12)
else null
end) <= covered_from_dt then 'R'
when activitypremium_src.activitysourcesuspensereferencenumber is not null
and (case
when lower(original_effective_dt) != 'null' then add_months(original_effective_dt,12)
else null
end) >= to_date(covered_to_dt) then 'F'
when covered_from_dt < (case
when lower(original_effective_dt) != 'null' then add_months(original_effective_dt,12)
else null
end)
and (case
when lower(original_effective_dt) != 'null' then add_months(original_effective_dt,12)
else null
end) < to_date(covered_to_dt) then 'F-R'
end as FRST_YR_RNWL_IND,
case
when (transactiondeviator like 'X13%'
or transactiondeviator like 'X15%'
or transactiondeviator like 'X23%'
or transactiondeviator like 'X33%'
or transactiondeviator like 'X35%'
or transactiondeviator like 'X36%'
or transactiondeviator like 'X38%'
or transactiondeviator like 'D6C7C7%')
and gl.transaction_type_cd in ('1') then 'D'
when (transactiondeviator like 'X13%'
or transactiondeviator like 'X15%'
or transactiondeviator like 'X23%'
or transactiondeviator like 'X33%'
or transactiondeviator like 'X35%'
or transactiondeviator like 'X36%'
or transactiondeviator like 'X38%'
or transactiondeviator like 'D6C7C7%')
and gl.transaction_type_cd in ('0') then 'C'
else 'B'
end as activityaccountingbalancedentryindicator,
Concat_ws('_',activitypremium_src.fgd_gl_detail_id,activitypremium_src.policy_id,activitypremium_src.activitysourcesystembatchidentifier
,activitypremium_src.activitysourcesystemactivityid) as sourceactivityid,
Concat_ws('_',activitypremium_src.policy_id,activitypremium_src.activitysourcesystembatchidentifier,
activitypremium_src.activitysourcesystemactivityid)
as activitysourceactivityparentid,
case
when m4.return_val is null then '0'
else m4.return_val
end as activityreversalcode,
case
when m2.return_val in ('Exclude') then 'Exclude'
when m2.return_val in ('Include')
and ( transactiondeviator like 'X13%'
or transactiondeviator like 'X15%'
or transactiondeviator like 'X23%'
or transactiondeviator like 'X25%'
or transactiondeviator like 'X33%'
or transactiondeviator like 'X35%'
or transactiondeviator like 'X43%'
or transactiondeviator like 'X45%'
or transactiondeviator like 'X53%'
or transactiondeviator like 'X55%'
or transactiondeviator like 'X63%'
or transactiondeviator like 'X65%'
or transactiondeviator like 'X73%'
or transactiondeviator like 'X75%'
or transactiondeviator like 'D6C77C%')
and gl_transaction_type_cd in ('1','2') then activitypremium_src.fgd_gl_account_cd
else m2.return_val
end as c_activitysourcetransactioncode,
case when activitypremium_src.pv_policy_id is not null and substr(activitypremium_src.ContractNumber,0,2) is not null then substr(activitypremium_src.ContractNumber,0,2)
when activitypremium_src.ActivitySourceDepositIdentifier is not null and substr(activitypremium_src.Bank_Account_Name,0,5)
is not null then substr(activitypremium_src.Bank_Account_Name,0,5)
else '0' end as c_src_lgl_enty
from
activitypremium_src
left join LKP_SRC_TXN_INCL_EXCL m2 on
activitypremium_src.transactiondeviator = m2.source_val
left join LKP_TNDEVIATOR m4 on
m4.source_val = activitypremium_src.transactiondeviator presrc
left join (
select
1 as n
union all
select
2) nums on
length(presrc.FRST_YR_RNWL_IND) >= nums.n) src
left join LKP_ACTVTY_TYP_TYPGRP m5 on
src.c_activitysourcetransactioncode = m5.actvty_src_trn_cd
and src.activityreversalcode = m5.actrscsys_acg_bal_ent_ind /*Wildcard implementation*/
and ( src.ACTVTY_FRST_YR_RNWL_IND in ('F','R')
or m5.ACTVTY_FRST_YR_RNWL_IND in ('F','R')
and src.ACTVTY_FRST_YR_RNWL_IND = ''))
left join LKP_SRCSYSMNT lkp_src_sys_nm on
lkp_src_sys_nm.CNTRT_SRC_SYS_NM_SRC = 'LTCG'
left join lkp_actvty_typ_typgrp lkp_typ_grp on
src.c_activitysourcetransactioncode = lkp_typ_grp.actvty_src_trn_cd
and src.activityreversalcode = lkp_typ_grp.actvty_rev_cd
and 'B' = lkp_typ_grp.actsrscsys_acg_bal_ent_ind
AND lkp_typ_grp.ACTVTY_SNGL_PRM_IND = '-'
AND lkp_typ_grp.ACTVTY_ACCTNG_MEMO_CD = '-'
AND lkp_typ_grp.ACTVTY_FRST_YR_RNWL_IND='-'
left join LKP_STAT_CMPY_TRNSLATN trans on
trim(src.c_src_lgl_enty) = trans.ORIGINTG_LGL_ENTY
LEFT JOIN LKP_LGL_ENTY_CD enty on
enty.SRC_LGL_ENTY_CD = trans.SRC_LGL_ENTY
left join lkp_cntrt_admn_loc_cd LOC_CD on
LOC_CD.CNTRT_SRC_SYS_NM_SRC = coalesce (lkp_src_sys_nm.CNTRT_SRC_SYS_NM,'@')
left join lkp_amt_method amt on
amt.ACT_SRC_SYS_NAME='LTCG'
and src.transactiondeviator = amt.SRC_ACT_MNY_MTHD
left join lkp_actvty_gl ACTVTY_GL on
ACTVTY_GL.ACTVTY_SRC_SYS_NM = 'LTCG'
where coalesce(src.c_activitysourcetransactioncode,'@') != 'Exclude')otr