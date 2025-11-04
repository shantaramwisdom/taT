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
       c.sourcesystemname as contractsourcesystemname,
       c.contractstatuscode,
       c.contractifrs17grouping as ifrs17grouping,
       c.contractifrs17portfolio as ifrs17portfolio,
       c.contractifrs17cohort as ifrs17cohort,
       c.contractifrs17profitability as ifrs17profitability,
       c.contractplancode as plancode,
       c.contractreportinglineofbusiness as contractsourceaccountingcenterdeviator,
       p.partylocationstatecode as statutoryresidentstatecode,
       case
           when c.source_system_name in ('ltcg', 'ltcghybrid', 'Bestow') then p.partyprimarypostalcode
           else null
       end as primaryinsuredprimarypostalcode,
       case
           when c.source_system_name = 'VantageP65' then p.partynonresidentaltaxstatus
           else null
       end as primaryannuitantnonresidentaltaxstatus,
       case
           when c.source_system_name = 'VantageP65' then p2.partynonresidentaltaxstatus
           else null
       end as ownernonresidentaltaxstatus,
       a.activitypostedglbatchidentifier,
       a.activityreversalcode,
       a.activitysourcelegalidentifier,
       a.activitysourceoriginatingresid,
       a.activitywithholdingtaxjurisdiction,
       a.activitychecknumber as checknumber,
       a.contractissuedate,
       c.contractissuerstatecode,
       c.contractsourcemarketorganizationcode,
       cast(a.activityamount as numeric(18,2)) as default_amount,
       a.sourceagentidentifier,
       a.fundsourcefundidentifier,
       a.activity1035exchangeindicator as "1035Exchangeindicator",
       a.activitysourceaccountingmnecode,
       c.contractdurationyears,
       a.activityeffectivedate,
       c.modelingrenewalpremium as contractmodelpremium,
       c.contractproductfundtotalityexpensecode,
       a.activitydistributionreasoncode as distributionreasoncode,
       a.fundnumber,
       c.contractgroupindicator,
       case
           when c.source_system_name in ('VantageP65', 'VantageP6', 'VantageP5') then c.contractpayoutoptionindicator1
           else null
       end as payoutoptionindicator,
       a.activitysourcelegalentitycode as sourcelegalentitycode,
       a.activityeffectivedate,
       c.contractbillingmode,
       case
           when c.source_system_name in ('ltcg', 'ltcghybrid') then a.activityaccountingbalanceentryindicator
           else 'B'
       end as activityaccountingbalanceentryindicator,
       a.activitysourceaccountingidentifier,
       c.contractproducttypecode as contractproducttypecode,
       a.activitymoneymethodcode as activitymoneymethodcode,
       a.activityunits as fundunits,
       case
           when a.sourcesystemname like 'VantageP%' then financedworchestration.uuid5(
                nvl(cast(a.pointofviewstartdate as varchar), '') + ':' + nvl(a.sourcesystemname, '') + ':' +
                nvl(a.sourceactivityid, '') + ':' + nvl(a.activitytype, '') + ':' + nvl(a.activityamounttype, '')
            )
           else a.activityaccountingid
       end as sourceaccountingid
from financedw.contract c
right join financedw.activity a on c.documentid = a.fkcontractdocumentid
left join financedw.partycontractrelationship pc on (a.source_system_name) in ('ltcg', 'ltcghybrid', 'Bestow')
     and pc.fkcontractdocumentid = c.documentid
     and pc.relationshiptype = 'Primary Insured'
     and ('{cycle_date}') between pc.pointofviewstartdate and pc.pointofviewstopdate
left join financedw.party p on (
            ( '{source_system_name}' in (
                'VantageP5',
                'VantageP6',
                'VantageP65',
                'VantageP75'
              )
              and p.documentid = c.primaryannuitantpartydocumentid)
         or ( '{source_system_name}' in ('ltcg', 'ltcghybrid', 'Bestow')
              and p.documentid = pc.fkpartydocumentid)
     )
     and ('{cycle_date}') between p.pointofviewstartdate and p.pointofviewstopdate
left join financedw.party p2 on ('{source_system_name}' = 'VantageP65')
     and p2.documentid = c.ownerpartydocumentid
     and ('{cycle_date}') between p2.pointofviewstartdate and p2.pointofviewstopdate
where (line_disable)
      and c.source_system_name = '{source_system_name}'
      and a.source_system_name = '{source_system_name}'
      and a.cycledate = '{cycle_date}'
      and a.batchid = {batchid}
      and ('{cycle_date}') between c.pointofviewstartdate and c.pointofviewstopdate