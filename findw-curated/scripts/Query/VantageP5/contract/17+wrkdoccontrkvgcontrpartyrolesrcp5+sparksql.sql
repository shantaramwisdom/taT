with delta_gb as
(select
 distinct key_qualifier,
 company_code,
 operation_code,
 cast(batchid as INT) batchid
 from
 gdqp5deltaextract_mainfile_keys
 where
 operation_code != 'DELT'
 and segment_id in ('GA',
 'GB')),
 delta_db as
(select
 distinct key_qualifier,
 company_code,
 system_name
 from
 deltaunion
 where
 cast(batchid as INT) = {batchid}
 and operation_code != 'DELT')
select
 uiddocumentgroupkey,
 max(case when partysourcerelationshiptypecode in ('ASG') then documentid
 else null end) as assigneepartydocumentid,
 max(case when partysourcerelationshiptypecode in ('CON') then documentid
 else null end) as jointownerpartydocumentid,
 max(case when partysourcerelationshiptypecode in ('OWN','PYI') then documentid
 else null end) as ownerpartydocumentid,
 max(case when partysourcerelationshiptypecode in ('PAR') then documentid
 else null end) as primaryannuitantpartydocumentid,
 max(case when partysourcerelationshiptypecode in ('INS') then documentid
 else null end) as primaryinsuredpartydocumentid,
 max(case when partysourcerelationshiptypecode in ('COA','SPA') then documentid
 else null end) as secondaryannuitantpartydocumentid,
 max(case when partysourcerelationshiptypecode in ('JNT') then documentid
 else null end) as secondaryinsuredpartydocumentid,
 max(case when partysourcerelationshiptypecode in ('SPS') then documentid
 else null end) as spousepartydocumentid
from
(select
 refparty.documentid,
 a.*
 from
 (select
 generatesameuuid(concat(delta2.system_name,
 ':',
 db.key_qualifier,
 ':',
 db.company_code)) as uiddocumentgroupkey,
 gb.role_code as partysourcerelationshiptypecode,
 row_number() over(partition by db.key_qualifier,
 gb.company_code,
 gb.role_code
 order by
 cast(delta.batchid as int) desc,
 cast(ga.batchid as int) desc,
 cast(gb.batchid as int) desc,
 cast(db.batchid as int) desc,
 gb.directory_id desc) as seq_num,
 case
 when ga.company_individual_code = 'I' then generatesameuuid(concat(delta2.system_name,
 ':',
 db.key_qualifier,
 ':',
 db.company_code,
 ':',
 ga.first_name,
 ':',
 ga.last_name))
 when ga.company_individual_code = 'C' then generatesameuuid(concat(delta2.system_name,
 ':',
 db.key_qualifier,
 ':',
 db.company_code,
 ':',
 ga.company_name))
 end as uiddocumentgroupkeyp5party
 from
 delta_gb as delta
 join gdqp5directsegmentgb_role_identification gb
 on
 gb.directory_id = delta.key_qualifier
 and gb.company_code = delta.company_code
 join delta_db delta2 on
 delta2.key_qualifier = gb.master_number
 and delta2.company_code = gb.company_code
 join gdqp5directsegmentga ga on
 gb.directory_id = ga.directory_id
 and gb.company_code = ga.company_code
 join (
 select
 key_qualifier,
 company_code,
 operation_code,
 batchid
 from
 gdqp5clprodsegmentdb)db
 on
 gb.master_number = db.key_qualifier
 and
 gb.company_code = db.company_code)a
 left join refdocpartyp5 refparty
 on
 refparty.documentid = a.uiddocumentgroupkeyp5party
 where
 a.seq_num = 1)
group by
 uiddocumentgroupkey;
