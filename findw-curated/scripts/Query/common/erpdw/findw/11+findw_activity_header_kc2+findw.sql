select case
           when a.activityamount = 0
                or (a.activityamounttype = 'Forecast Premium'
                    and a.source_system_name like 'VantageP%')
                or a.activityreversalcode = 'S'
                or a.activityreporteddate != a.cycledate then 'exclude'
                else 'include'
       end as findw_include_exclude,
       a.activitygeneralglapplicationareacode as gl_application_area_code,
       a.activitygeneralglsourcecode as gl_source_code,
       a.activitysourcelegalentitycode as ledger_name,
       a.activityreporteddate as transaction_date,
       a.contractadministrationlocationcode,
       a.fundnumber,
       c.contractnumber,
       sha2(
            coalesce(a.activitytypegroup,'') || '|' ||
            coalesce(a.activitytype,'') || '|' ||
            coalesce(a.activityamounttype,'') || '|' ||
            coalesce(a.fundclassindicator,'') || '|' ||
            coalesce(c.contractnumber,'') || '|' ||
            coalesce(c.sourcesystemname,'') || '|' ||
            coalesce(c.contractifrs17grouping,'') || '|' ||
            coalesce(c.contractifrs17portfolio,'') || '|' ||
            coalesce(c.contractifrs17profitability,'') || '|' ||
            coalesce(c.contractifrs17cohort,'') || '|' ||
            coalesce(c.contractplancode,'') || '|' ||
            coalesce(a.activitysourcelegalentitycode,'') || '|' ||
            coalesce(a.activitygeneralglapplicationareacode,'') || '|' ||
            coalesce(a.activitygeneralglsourcecode,'') || '|' ||
            coalesce(cast(a.activityreporteddate as varchar), '') || '|' ||
            findw_include_exclude || '|' ||
            cast(a.cycledate as varchar) || '|' ||
            cast(a.batchid as varchar), 256) as activity_key
from financedw.contract c
right join financedw.activity a on c.documentid = a.fkcontractdocumentid
where (header_disable)
      and c.source_system_name = '{source_system_name}'
      and a.source_system_name = '{source_system_name}'
      and a.cycledate = '{cycle_date}'
      and a.batchid = {batchid}
      and '{cycle_date}' between c.pointofviewstartdate and c.pointofviewstopdate
group by 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9
