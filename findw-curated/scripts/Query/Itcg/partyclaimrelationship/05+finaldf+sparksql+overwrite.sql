select
from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') AS recorded_timestamp,
'{source_system_name}' AS source_system_name,
SHA2(concat(COALESCE(m1.sourcepartyid, ''),COALESCE(m1.sourceclaimidentifier, ''),':LTCG:'),256) as documentid,
case
when m3.Include_Exclude = 'Include' then m3.RLTNSP_TYP
when m3.Include_Exclude = 'Exclude' then '#'
ELSE '*'
END ,256) as documentid,
SHA2(concat(COALESCE(m1.sourcepartyid, ''),':LTCG'),256) as fkpartydocumentid,
SHA2(concat(COALESCE(m1.sourceclaimidentifier, ''),':LTCG'),256) as fkclaimdocumentid,
m1.sourcepartyid,
m1.sourceclaimidentifier,
case
when m3.Include_Exclude = 'Include' then m3.RLTNSP_TYP
when m3.Include_Exclude = 'Exclude' then '#'
end as RelationshipType,
'LTCG' as sourcesystemname,
m1.contractnumber,
coalesce(m4.CNTRT_ADMN_LOC_CD,'@') as ContractAdministrationLocationCode,
cast(m1.cycle_date as date) as cycle_date,
cast(m1.batchid as int) as batch_id
from
partyclaimrelationship_src m1
left join sourcesystemname m2 on
m2.CNTRT_SRC_SYS_NM_SRC = 'LTCG'
left join (
select
LKP_RLTNSP_TYP.Include_Exclude,
LKP_RLTNSP_TYP.SRC_RLTNSP_TYP,
LKP_RLTNSP_TYP.RLTNSP_TYP
from LKP_RLTNSP_TYP
where
LKP_RLTNSP_TYP.CNTRT_OPT_GSI_IND_INT = '~'
and LKP_RLTNSP_TYP.SRC_INPUT_TYP_SEQ = '~'
and LKP_RLTNSP_TYP.CNTRT_SRC_SYS_ORD_POS = '~'
) m3 on
m3.SRC_RLTNSP_TYP = m1.SRC_RLTNSP_TYP
left join LKP_CNTRT_ADMN_LOC_CD m4 on
coalesce(m2.CNTRT_SRC_SYS_NM,'@') = m4.CNTRT_SRC_SYS_NM_SRC
and m4.ADMIN_PRCSS_LEG_ENTY_CD_SRC = '~'
