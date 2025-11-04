SELECT a.uiddocumentgroupkey,
a.fundrevenuesharingfee,
a.fundfacilitationfee,
a.fundinvestmentmanagementfee,
a.fundmerrilllynchrevenuesharingfee,
a.fundmerrilllynchfundfacilitationfee,
a.fundisharefundfacilitationfee,
a.load_date,
a.batchid
FROM (SELECT generatesameuuid(concat(alm.fundnumber,':', alm.fundregion)) as uiddocumentgroupkey,
coalesce(alm.rs_override, alm.c_rs, alm.rs) as fundrevenuesharingfee,
coalesce(alm.fff_override, alm.c_fff, alm.fff) as fundfacilitationfee,
coalesce(alm.imf_override, alm.c_imf, alm.imf) as fundinvestmentmanagementfee,
coalesce(alm.ml_rs_override,alm.c_ml_rs,alm.ml_rs) as fundmerrilllynchrevenuesharingfee,
coalesce(alm.ml_fff_override,alm.c_ml_fff,alm.ml_fff) as fundmerrilllynchfundfacilitationfee,
coalesce(alm.ishare_fff_override,alm.c_ishare_fff,alm.ishare_fff) as fundisharefundfacilitationfee,
'{cycle_date}' as load_date,
{batchid} as batchid,
row_number() OVER(PARTITION BY alm.fundnumber,alm.fundregion ORDER BY alm.time_stamp desc, cast(alm.batchid as int) desc) as seq_num
FROM (SELECT * FROM {source_database}.gdqalmfundmapping WHERE cast(batchid as int) = {batchid}) alm)a
WHERE a.seq_num = 1;