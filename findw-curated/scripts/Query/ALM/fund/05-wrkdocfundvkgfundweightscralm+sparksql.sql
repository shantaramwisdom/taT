SELECT a.uiddocumentgroupkey,
a.fundseparateaccountweightindex1,
a.fundseparateaccountweightindex2,
a.fundseparateaccountweightindex3,
a.fundseparateaccountweightindex4,
a.fundseparateaccountweightindex5,
a.fundseparateaccountweightindex6,
a.fundseparateaccountweightindex7,
a.fundseparateaccountweightindex8,
a.fundseparateaccountweightindex9,
a.fundseparateaccountweightindex10,
a.load_date,
a.batchid
FROM (SELECT generatesameuuid(concat(alm.fundnumber,':', alm.fundregion)) as uiddocumentgroupkey,
coalesce(alm.sa1_override, alm.c_sa1, alm.sa1) as fundseparateaccountweightindex1,
coalesce(alm.sa2_override, alm.c_sa2, alm.sa2) as fundseparateaccountweightindex2,
coalesce(alm.sa3_override, alm.c_sa3, alm.sa3) as fundseparateaccountweightindex3,
coalesce(alm.sa4_override, alm.c_sa4, alm.sa4) as fundseparateaccountweightindex4,
coalesce(alm.sa5_override, alm.c_sa5, alm.sa5) as fundseparateaccountweightindex5,
coalesce(alm.sa6_override, alm.c_sa6, alm.sa6) as fundseparateaccountweightindex6,
coalesce(alm.sa7_override, alm.c_sa7, alm.sa7) as fundseparateaccountweightindex7,
coalesce(alm.sa8_override, alm.c_sa8, alm.sa8) as fundseparateaccountweightindex8,
coalesce(alm.sa9_override, alm.c_sa9, alm.sa9) as fundseparateaccountweightindex9,
coalesce(alm.sa10_override,
alm.c_sa10,
alm.sa10) as fundseparateaccountweightindex10,
'{cycle_date}' as load_date,
{batchid} as batchid,
row_number() OVER(PARTITION BY alm.fundnumber,
alm.fundregion
ORDER BY alm.time_stamp desc, cast(alm.batchid as int) desc) as seq_num
FROM (SELECT *
FROM {source_database}.gdqalmfundmapping
WHERE cast(batchid as int) = {batchid}) alm)a
WHERE a.seq_num = 1;