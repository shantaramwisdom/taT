SELECT from_utc_timestamp(current_timestamp(),'IST') as load_date,
       '{source_system_name}' as source_system_name,
       wrk1.uiddocumentgroupkey as documentid,
       cast(wrk3.contractdocumentid as string) as fkcontractdocumentid,
       '{source_system_name}' as sourcesystemname,
       wrk1.contractnumber,
       wrk1.contractadministrationlocationcode,
       cast(wrk1.source_system_ordinal_position as integer) as contractoptionsourcesystemordinalposition,
       cast(wrk2.contractoptioneffectivedate as timestamp) as contractoptioneffectivedate,
       cast(wrk2.contractoptionbenefitselectiondate as timestamp) as contractoptionbenefitselectiondate,
       cast(wrk2.contractoptionbenefitsoptdate as timestamp) as contractoptionbenefitsoptdate,
       cast(wrk3.contractoptionstatuscode as string) as contractoptionstatuscode,
       cast(wrk3.contractoptionmaximumanniversaryvalue as numeric(18,6)) as contractoptionmaximumanniversaryvalue,
       cast(wrk3.contractoptionbenefitsoptindicator as string) as contractoptionbenefitsoptindicator,
       cast(wrk3.contractoptionissueage as integer) as contractoptionissueage,
       cast(wrk3.contractoptiondurationinmonths as integer) as contractoptiondurationinmonths,
       cast(wrk3.contractoptionsinglejointindicator as string) as contractoptionsinglejointindicator,
       cast(wrk3.contractoptiondeathindicator as string) as contractoptiondeathindicator,
       cast(wrk3.contractoptionincomeenhancementindicator as string) as contractoptionincomeenhancementindicator,
       cast(wrk3.contractoptionmodelingpwvbsegment as integer) as contractoptionmodelingpwvbsegment,
       cast(wrk3.contractoptionmodelingtypegroupindicator as string) as contractoptionmodelingtypegroupindicator,
       cast(wrk4.contractoptiontypegroup as string) as contractoptiontypegroup,
       cast(wrk4.contractoptionplancode as string) as contractoptionplancode,
       cast(wrk4.contractoptiongenerationid as integer) as contractoptiongenerationid,
       cast(wrk4.contractoptionissuestatecode as string) as contractoptionissuestatecode,
       cast(wrk4.contractoptionreturnofpremiumvalue as numeric(18,6)) as contractoptionreturnofpremiumvalue,
       cast(wrk4.contractoptionstepvalue as numeric(18,6)) as contractoptionstepvalue,
       wrk1.operationcode,
       cast('{cycle_date}' as date) as cycle_date,
       cast('{batchid}' as int) as batch_id
FROM wrkdoccontractoptionkvgcontroptionnaturalkeysrcp5 wrk1
LEFT JOIN wrkdoccontractoptionkvgcontroptiondatesrcp5 wrk2
ON wrk1.uiddocumentgroupkey = wrk2.uiddocumentgroupkey
LEFT JOIN wrkdoccontractoptionkvgcontroptiondatasrcp5 wrk3
ON wrk1.uiddocumentgroupkey = wrk3.uiddocumentgroupkey
LEFT JOIN wrkdoccontractoptionkvgcontroptionproductscrp5 wrk4
ON wrk1.uiddocumentgroupkey = wrk4.uiddocumentgroupkey;
