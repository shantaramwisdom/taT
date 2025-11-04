SELECT a.uiddocumentgroupkey,
a.contractissuestatecode,
operationcode,
a.dttloaddate,
a.intbatchid
FROM (
SELECT generatesameuuid(concat(delta.system_name,':', db.key_qualifier,':', db.company_code)) as uiddocumentgroupkey,
db.policy_issue_state as contractissuestatecode,
db.operation_code as operationcode,
'{cycle_date}' as dttloaddate,
{batchid} as intbatchid,
row_number() OVER(PARTITION BY db.company_code,
db.segment_id,
db.key_qualifier
ORDER BY cast(delta.batchid as int) desc,
cast(db.batchid as int) desc,
db.segment_sequence_number desc
) as seq_num
FROM deltaunion delta
JOIN (
SELECT key_qualifier,
company_code,
batchid,
segment_id,
segment_sequence_number,
policy_issue_state,
operation_code,
contract_issue_country_code
FROM gdqp5clprodsegmentdb
WHERE cast(batchid as int) <= {batchid}
) db
ON delta.key_qualifier = db.key_qualifier
AND delta.company_code = db.company_code
) a
WHERE a.seq_num = 1;