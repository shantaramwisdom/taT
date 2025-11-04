select case
           when a.activityamount = 0
                or (a.activityamounttype = 'Forecast Premium'
                    and a.source_system_name like 'VantageP%')
                or a.activityreversalcode = 'S'
                or a.activityreporteddate != a.cycledate then 'exclude'
                else 'include'
       end as findw_include_exclude,
       count(*) count_rec,
       count(distinct a.documentid) count_activity_nk,
       sum(cast(a.activityamount as numeric(18,2))) sum_activityamount,
       count(distinct
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
            coalesce(cast(a.activityreporteddate as varchar),'') || '|' ||
            findw_include_exclude
       ) as count_distinct_activity_keys
from financedw.contract c
right join financedw.activity a on c.documentid = a.fkcontractdocumentid
where c.source_system_name = '{source_system_name}'
      and a.source_system_name = '{source_system_name}'
      and a.cycledate = '{cycle_date}'
      and a.batchid = {batchid}
      and '{cycle_date}' between c.pointofviewstartdate and c.pointofviewstopdate
group by 1
