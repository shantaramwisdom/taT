SELECT a.uiddocumentgroupkey,
a.fundnumber,
a.fundregion,
a.fundname,
cast(a.operationcode as string) as operationcode,
a.load_date,
a.batchid
FROM (SELECT alm.fundnumber as fundnumber,
alm.fundregion as fundregion,
alm.fundname as fundname,
concat(alm.fundnumber,':', alm.fundregion) as fund_concatid,
generatesameuuid(concat(alm.fundnumber,':', alm.fundregion)) as uiddocumentgroupkey,
'{cycle_date}' as load_date,
{batchid} as batchid,
null as operationcode,
row_number() OVER(PARTITION BY alm.fundnumber,
alm.fundregion
ORDER BY alm.time_stamp desc, cast(alm.batchid as int) desc) as seq_num
FROM (SELECT *
FROM {source_database}.gdqalmfundmapping
WHERE cast(batchid as int) = {batchid}) alm)a
WHERE a.seq_num = 1;