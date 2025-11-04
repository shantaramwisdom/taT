SELECT a.uiddocumentgroupkey,
a.fundnumber,
a.fundregion,
a.fundname,
a.load_date,
a.batchid
FROM (SELECT generatesameuuid(concat(alm.fundnumber,':', alm.fundregion)) as uiddocumentgroupkey,
coalesce(alm.fundnumber_override, alm.c_fundnumber, alm.fundnumber) as fundnumber,
coalesce(alm.fundregion_override, alm.c_fundregion, alm.fundregion) as fundregion,
coalesce(alm.fundname_override, alm.c_fundname, alm.fundname) as fundname,
'{cycle_date}' as load_date,
{batchid} as batchid,
row_number() OVER(PARTITION BY alm.fundnumber,
alm.fundregion
ORDER BY alm.time_stamp desc, cast(alm.batchid as int) desc) as seq_num
FROM (SELECT *
FROM {source_database}.gdqalmfundmapping
WHERE cast(batchid as int) = {batchid}) alm)a
WHERE a.seq_num = 1;