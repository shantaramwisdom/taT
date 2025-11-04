SELECT from_utc_timestamp(CURRENT_TIMESTAMP,'US/Central') AS recorded_timestamp,
"ALM" as source_system_name,
wrk1.uiddocumentgroupkey as documentid,
'ALM' as sourcesystemname,
wrk1.fundnumber,
wrk1.fundregion,
wrk3.productfundticker,
wrk1.fundname,
wrk3.fundloadablefile,
wrk3.fundindicator,
cast(date_format(to_date(wrk3.fundstudydate,'yyyyMMdd'),'yyyy-MM-dd') as date) as fundstudydate,
cast(date_format(to_date(wrk3.fundinforcedate,'yyyyMMdd'),'yyyy-MM-dd') as date) as fundinforcedate,
wrk3.fundcompany,
wrk3.fundcusip,
wrk3.fundcusipticker,
wrk3.fundtypeid,
wrk3.fundcategory,
wrk3.fundclass,
wrk3.funddynamicseparateaccountflag,
wrk3.fundselfhedgingseparateaccountflag,
wrk3.fundgainlossfee,
wrk4.fundrevenuesharingfee,
wrk4.fundfacilitationfee,
wrk4.fundinvestmentmanagementfee,
wrk4.fundmerrilllynchrevenuesharingfee,
wrk4.fundmerrilllynchfundfacilitationfee,
wrk4.fundisharefundfacilitationfee,
wrk5.fundseparateaccountweightindex1,
wrk5.fundseparateaccountweightindex2,
wrk5.fundseparateaccountweightindex3,
wrk5.fundseparateaccountweightindex4,
wrk5.fundseparateaccountweightindex5,
wrk5.fundseparateaccountweightindex6,
wrk5.fundseparateaccountweightindex7,
wrk5.fundseparateaccountweightindex8,
wrk5.fundseparateaccountweightindex9,
wrk5.fundseparateaccountweightindex10,
wrk1.operationcode,
CAST('{cycle_date}' AS date) AS cycle_date,
CAST({batchid} AS int) AS batch_id
FROM wrkdocfundvkgfundscralm wrk1
LEFT JOIN wrkdocfundvkgfundnaturalkeyscralm wrk2
ON wrk1.uiddocumentgroupkey = wrk2.uiddocumentgroupkey
LEFT JOIN wrkdocfundvkgfunddatascralm wrk3
ON wrk1.uiddocumentgroupkey = wrk3.uiddocumentgroupkey
LEFT JOIN wrkdocfundvkgfundfeesscralm wrk4
ON wrk1.uiddocumentgroupkey = wrk4.uiddocumentgroupkey
LEFT JOIN wrkdocfundvkgfundweightscralm wrk5
ON wrk1.uiddocumentgroupkey = wrk5.uiddocumentgroupkey;