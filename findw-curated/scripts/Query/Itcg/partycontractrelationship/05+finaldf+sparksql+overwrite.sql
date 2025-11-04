select
from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') AS recorded_timestamp,
'{source_system_name}' AS source_system_name,
SHA2(concat(COALESCE(sourcepartyid, ''), ':', COALESCE(contratnumber, ''), ':', COALESCE(LKP_CNTRT_ADMN_LOC_CD_M.CNTRT_ADMN_LOC_CD, ''), ':LTCG', COALESCE( case
when LKP_RLTNSP_TYP_M.INCLUDE_EXCLUDE = 'Include' then LKP_RLTNSP_TYP_M.RLTNSP_TYP
when LKP_RLTNSP_TYP_M.INCLUDE_EXCLUDE = 'Exclude' then '#'
else null
end ), 256) AS documentid,
SHA2(concat(COALESCE(LKP_CNTRT_ADMN_LOC_CD_M.CNTRT_ADMN_LOC_CD, ''), ':LTCG'), 256) AS fkpartydocumentid,
SHA2(concat(COALESCE(sourcepartyid, ''), ':LTCG'), 256) AS fkcontractdocumentid,
SRC.SourcePartyID as SourcePartyID,
case
when LKP_RLTNSP_TYP_M.INCLUDE_EXCLUDE = 'Include' then LKP_RLTNSP_TYP_M.RLTNSP_TYP
when LKP_RLTNSP_TYP_M.INCLUDE_EXCLUDE = 'Exclude' then '#'
else null
end as relationshiptype,
'LTCG' as sourcesystemname,
SRC.ContractNumber as contractnumber,
LKP_CNTRT_ADMN_LOC_CD_M.CNTRT_ADMN_LOC_CD as contractadministrationlocationcode,
cast(SRC.cycle_date as date) as cycle_date,
cast(SRC.batch_id as int) as batch_id
from
party_contract_relationship_src SRC
left join LKP_CNTRT_ADMN_LOC_CD LKP_CNTRT_ADMN_LOC_CD_M on
'LTCG' = LKP_CNTRT_ADMN_LOC_CD_M.CNTRT_SRC_SYS_NM_SRC
left join LKP_RLTNSP_TYP LKP_RLTNSP_TYP_M on
'LTCG' = LKP_RLTNSP_TYP_M.SRC_SYS_NM
and SRC.SRC_RLTNSP_TYP = LKP_RLTNSP_TYP_M.SRC_RLTNSP_TYP
