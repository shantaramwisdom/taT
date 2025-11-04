select case
           when a.activityamount = 0
                or (a.activityamounttype = 'Forecast Premium'
                    and a.source_system_name like 'VantageP%')
                or a.activityreversalcode = 'S'
                or a.activityreporteddate != a.cycledate then 'exclude'
                else 'include'
       end as findw_include_exclude,
       a.activitytypegroup,
       a.activitytype,
       a.activityamounttype,
       a.fundclassindicator,
       a.activitygeneralglapplicationareacode as gl_application_area_code,
       a.activitygeneralglsourcecode as gl_source_code,
       a.activitysourcelegalentitycode as ledger_name,
       a.activityreporteddate as transaction_date,
       a.contractadministrationlocationcode,
       a.fundnumber,
       a.contractnumber,
       c.sourcesystemname as source_system_nm,
       c.contractifrs17grouping as ifrs17grouping,
       c.contractifrs17portfolio as ifrs17portfolio,
       c.contractifrs17profitability as ifrs17profitability,
       c.contractifrs17cohort as ifrs17cohort,
       c.contractplancode as plancode
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
         9,
         10,
         11,
         12,
         13,
         14,
         15,
         16,
         17,
         18