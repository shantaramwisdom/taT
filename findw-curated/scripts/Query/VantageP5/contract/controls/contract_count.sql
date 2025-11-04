WITH gdqpsdeltaextract_mainfile_keys as
(
(SELECT key_qualifier,
company_code,
segment_id,
system_name,
segment_sequence_number,
file_name,
batchid,
seq_num,
CASE
WHEN operation_code = 'DELT' THEN 'DELT'
ELSE 'NON-DELT'
END AS operation_code
FROM
(SELECT key_qualifier,
company_code,
segment_id,
system_name,
segment_sequence_number,
file_name,
batchid,
operation_code,
row_number() over(PARTITION BY key_qualifier, company_code, segment_id, CASE
WHEN segment_id ='JL' THEN segment_sequence_number
ELSE NULL
END
ORDER BY cast(batchid AS int) DESC, (CASE
WHEN operation_code = 'DELT' THEN 2
ELSE 1
END)) AS seq_num
FROM {source_database}.gdqpsdeltaextract_mainfile_keys
WHERE cast(batchid AS int) <= {batchid}) del
WHERE del.seq_num = 1
AND ((file_name like 'CLPROD%'
AND segment_id in ('JL',
'DB'))
OR (file_name = 'DIRECT'
AND segment_id in ('GB',
'GA'))
OR (file_name like 'SCHED'))
),
db as
(
(SELECT db.*,
case when dmk.operation_code = 'DELT' then 'DELT'
else 'NON-DELT' end as operation_code
FROM
(SELECT uuid ,
segment_id ,
segment_sequence_number ,
management_code ,
coalesce(company_code_override, c_company_code, company_code)   as company_code ,
coalesce(key_qualifier_override,
c_key_qualifier)                as key_qualifier ,
coalesce(statutory_company_code_override,
c_statutory_company_code,
statutory_company_code) as statutory_company_code ,
coalesce(plan_code_override, c_plan_code, plan_code)     as plan_code ,
batchid ,
row_number() OVER(PARTITION BY key_qualifier,
company_code,
segment_id,
ordinal_position
ORDER BY cast(batchid as int) desc, segment_sequence_number desc) as row_num
FROM (source_database).gdqpsclprodsegmentdb
WHERE cast(batchid as int) <= {batchid}) db
WHERE row_num = 1) db
LEFT JOIN (SELECT * from gdqp5deltaextract_mainfile_keys
WHERE cast(batchid as int) <= {batchid}
and file_name like 'CLPROD%'
and segment_id = 'DB' and
seq_num = 1) dmk
ON db.key_qualifier = dmk.key_qualifier
AND db.company_code = dmk.company_code),
vantagedelta as
(
SELECT a.*
FROM (SELECT *
FROM   (source_database).gdqvantageoneacctctr
WHERE  batchid = '{batchidvantage}') a
LEFT JOIN (SELECT *
FROM   (source_database).gdqvantageoneacctctr
WHERE  batchid = '{batchidvantagetwo}') b
ON a.company = b.company and
a.plan = b.plan and
a.mgt_code = b.mgt_code
WHERE concat(coalesce(a.center_number,'NULL'), coalesce(a.c_center_number,'NULL'), coalesce(a.center_number_isvalid,'NULL'),
coalesce(a.center_number_override,'NULL')) <> concat(coalesce(b.center_number,'NULL'), coalesce(b.c_center_number,'NULL'),
coalesce(b.center_number_isvalid,'NULL'), coalesce(b.center_number_override,'NULL'))
),
SELECT (batchid) batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'BALANCING_COUNT' measure_name,
'curated' hop_name,
'recordcount' measure_field,
'INTEGER' measure_value_datatype,
'S' measure_src_tgt_adj_indicator ,
'gdqp5deltaextract_mainfile_keys' measure_table ,
count(1) measure_value
FROM (SELECT db.operation_code as operationcode,
row_number() OVER(PARTITION BY db.company_code,
db.segment_id,
db.key_qualifier
ORDER BY cast(dmk.batchid as int) desc, cast(db.batchid as int) desc, db.segment_sequence_number desc) as seq_num
FROM (SELECT a.key_qualifier,
a.company_code,
a.system_name,
a.batchid,
a.operation_code,
a.segment_id
FROM (SELECT db.key_qualifier,
db.company_code,
db.system_name,
db.batchid,
db.operation_code,
db.segment_id,
row_number() OVER(PARTITION BY db.company_code,
db.segment_id,
db.key_qualifier,
db.segment_sequence_number
ORDER BY cast(db.batchid as int) desc) as row_num
FROM (SELECT db.key_qualifier,
db.company_code,
db.system_name,
db.batchid,
db.operation_code,
db.segment_id,
row_number() OVER(PARTITION BY db.company_code,
db.segment_id,
db.key_qualifier
ORDER BY cast(db.batchid as int) desc) as rn
FROM db) db
WHERE cast(dmk.batchid as int) = {batchid}
and dmk.system_name = 'P5'
and ((dmk.file_name like 'CLPROD%'
AND dmk.segment_id in ('DB','JL'))
OR (dmk.file_name = 'DIRECT')
OR (dmk.file_name like 'SCHED'))) dmk
WHERE a.seq_num = 1) a) )
UNION (SELECT key_qualifier,
company_code,
system_name,
batchid,
operation_code,
segment_id
FROM (SELECT a.key_qualifier,
a.company_code,
a.system_name,
a.batchid,
a.operation_code,
a.segment_id
FROM (SELECT db.key_qualifier,
db.company_code,
db.system_name,
db.batchid,
db.operation_code,
db.segment_id,
row_number() OVER(PARTITION BY db.company_code,
db.segment_id,
db.key_qualifier,
db.segment_sequence_number
ORDER BY cast(db.batchid as int) desc) as row_num
FROM db) db
WHERE a.seq_num = 1)
)
UNION (SELECT dmk.key_qualifier,
dmk.company_code,
dmk.system_name,
dmk.batchid,
dmk.operation_code,
dmk.segment_id
FROM (SELECT key_qualifier,
company_code,
system_name,
batchid,
operation_code,
segment_id,
row_number() OVER(PARTITION BY company_code,
segment_id,
key_qualifier
ORDER BY cast(batchid as int) desc) as rn
FROM gdqpsdeltaextract_mainfile_keys dmk
WHERE cast(dmk.batchid as int) = {batchid}
and dmk.system_name = 'P5'
and (dmk.file_name like 'CLPROD%'
or dmk.file_name like 'DIRECT'
or dmk.file_name like 'SCHED')) dmk2
WHERE rn = 1)
UNION (SELECT key_qualifier,
company_code,
system_name,
batchid,
operation_code,
segment_id
FROM (SELECT key_qualifier,
company_code,
system_name,
batchid,
operation_code,
segment_id,
row_number() OVER(PARTITION BY company_code,
segment_id,
key_qualifier
ORDER BY cast(batchid AS int) desc) as seq_num
FROM (source_database).gdqpsdeltaextract_mainfile_keys
WHERE cast(batchid as int) <= {batchid}
and file_name like 'CLPROD%'
and segment_id = 'DB') dmk)
UNION (SELECT dmk.key_qualifier,
dmk.company_code,
dmk.system_name,
dmk.batchid,
dmk.operation_code,
dmk.segment_id
FROM (SELECT db.key_qualifier,
db.company_code,
db.system_name,
db.batchid,
db.operation_code,
db.segment_id,
row_number() OVER(PARTITION BY db.company_code,
db.segment_id,
db.key_qualifier
ORDER BY cast(db.batchid as int) desc) as row_num
FROM db) db2
WHERE db2.row_num = 1)
UNION (SELECT dmk.key_qualifier,
dmk.company_code,
dmk.system_name,
dmk.batchid,
dmk.operation_code,
dmk.segment_id
FROM (SELECT key_qualifier,
company_code,
system_name,
batchid,
operation_code,
segment_id,
row_number() OVER(PARTITION BY company_code,
segment_id,
key_qualifier
ORDER BY cast(batchid as int) desc) as seq_num
FROM (source_database).gdqpsdeltaextract_mainfile_keys dmk
WHERE cast(dmk.batchid as int) <= {batchid}
and dmk.system_name = 'P5'
and ((dmk.file_name like 'CLPROD%' AND dmk.segment_id in ('DB'))
OR (dmk.file_name = 'DIRECT' AND dmk.segment_id in ('GB','GA'))
OR (dmk.file_name like 'SCHED'))) dmk_union
WHERE seq_num = 1)
)
