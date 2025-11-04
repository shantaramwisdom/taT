SELECT from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') as recorded_timestamp,
'{source_system_name}' as source_system_name,
wrk1.uiddocumentgroupkey as documentid,
'{source_system_name}' as sourcesystemname,
wrk1.contractnumber,
wrk1.contractadministrationlocationcode,
cast(wrk2.contractissuedate as date) as contractissuedate,
cast(wrk2.contractissuemonth as int) as contractissuemonth,
cast(wrk2.contractissueyear as int) as contractissueyear,
cast(wrk2.contracteffectiveyear as int) as contracteffectiveyear,
cast(wrk2.contracteffectivemonth as int) as contracteffectivemonth,
cast(wrk2.contracteffectivedate as date) as contracteffectivedate,
cast(wrk3.contractissueage as int) as contractissueage,
cast(wrk3.contractdurationinyears as int) as contractdurationinyears,
wrk3.contractgroupindicator,
wrk3.contractparticipationindicator,
wrk3.contractreportinglineofbusiness,
wrk3.residentstatecode as contractresidentstatecode,
wrk3.contractstatuscode,
wrk3.contractplancode,
wrk2.contractlastfinancetransactiondate,
wrk3.contractproducttypename,
wrk3.contractformcodespecialneeds,
cast(wrk3.contractnumbernumericid as bigint) as contractnumbernumericid,
wrk4.centernumber,
wrk4.sourcelegalentitycode,
wrk4.contractqualifiedindicator,
wrk4.contractdacgroup,
wrk4.contractlegalcompanycode,
wrk4.originatinglegalentitycode,
cast(wrk5.contracttotalaccountvalue as numeric(18, 6)) as contracttotalaccountvalue,
cast(wrk5.surrendervalueamount as numeric(18, 6)) as surrendervalueamount,
cast(wrk5.contractcumulativewithdrawals as numeric(18, 6)) as contractcumulativewithdrawals,
cast(wrk5.contractpolicyyeartodatecumulativewithdrawals as numeric(18, 6)) as contractpolicyyeartodatecumulativewithdrawals,
cast(wrk5.contracttotalseparateaccountvalue as numeric(18, 6)) as contracttotalseparateaccountvalue,
cast(wrk5.modelingrenewalpremium as numeric(18, 6)) as modelingrenewalpremium,
cast(wrk5.contractfreewithdrawalpercentage as numeric(18, 6)) as contractfreewithdrawalpercentage,
cast(wrk5.contractsurrenderchargepercentage as numeric(18, 6)) as contractsurrenderchargepercentage,
cast(wrk5.initialpremium as numeric(18, 6)) as initialpremium,
cast(wrk5.contractfundvaluetag as bigint) as contractfundvaluetag,
wrk6.contractsourcemarketingorganizationcode,
wrk6.contractmodelingbusinessunitgroup,
wrk6.contractifrs17portfolio,
wrk6.contractifrs17grouping,
wrk6.contractifrs17cohort,
wrk6.contractifrs17profitability,
wrk7.contractmodelingreinsuranceidentifier,
wrk8.assigneepartydocumentid,
wrk8.jointownerpartydocumentid,
wrk8.ownerpartydocumentid,
wrk8.primaryannuitantpartydocumentid,
wrk8.primaryinsuredpartydocumentid,
wrk8.secondaryannuitantpartydocumentid,
wrk8.secondaryinsuredpartydocumentid,
wrk8.spousepartydocumentid,
wrk9.contractissuestatecode,
wrk1.operationcode,
cast('{cycle_date}' as date) as cycle_date,
cast({batchid} as int) as batch_id
FROM wrkdoccontrkvgcontrnaturalkeysrcp5 wrk1
LEFT JOIN wrkdoccontrkvgcontrdatessrcp5 wrk2
ON wrk1.uiddocumentgroupkey = wrk2.uiddocumentgroupkey
LEFT JOIN wrkdoccontrkvgcontrdatasrcp5 wrk3
ON wrk1.uiddocumentgroupkey = wrk3.uiddocumentgroupkey
LEFT JOIN wrkdoccontrkvgcontrfinrptsrcp5 wrk4
ON wrk1.uiddocumentgroupkey = wrk4.uiddocumentgroupkey
LEFT JOIN wrkdoccontrkvgcontrfinrsrcp5 wrk5
ON wrk1.uiddocumentgroupkey = wrk5.uiddocumentgroupkey
LEFT JOIN wrkdoccontrkvgcontrsourceofbusinesssrcp5 wrk6
ON wrk1.uiddocumentgroupkey = wrk6.uiddocumentgroupkey
LEFT JOIN wrkdoccontrkvgcontrreinsuranceidsrcp5 wrk7
ON wrk1.uiddocumentgroupkey = wrk7.uiddocumentgroupkey
LEFT JOIN wrkdoccontrkvgcontrpartyrolessrcp5 wrk8
ON wrk1.uiddocumentgroupkey = wrk8.uiddocumentgroupkey
LEFT JOIN wrkdoccontrkvgcontrgeographysrcp5 wrk9
ON wrk1.uiddocumentgroupkey = wrk9.uiddocumentgroupkey;