with source as (
    select activitytypegroup,
           activitytype,
           activityamounttype,
           fundclassindicator,
           contractadministrationlocationcode,
           contractnumber,
           (source_system_ovrd_sum),
           ifrs17grouping,
           ifrs17portfolio,
           ifrs17profitability,
           ifrs17cohort,
           plancode,
           ledger_name,
           gl_application_area_code,
           gl_source_code,
           transaction_date,
           original_cycle_date,
           original_batch_id,
           load_type,
           findw_include_exclude,
           collect_list(fundnumber) as fundnumber_array
           (line_sum_amount)
    from (
            select activitytypegroup,
                   activitytype,
                   activityamounttype,
                   fundclassindicator,
                   contractadministrationlocationcode,
                   contractnumber,
                   (source_system_ovrd),
                   ifrs17grouping,
                   ifrs17portfolio,
                   ifrs17profitability,
                   ifrs17cohort,
                   plancode,
                   ledger_name,
                   gl_application_area_code,
                   gl_source_code,
                   fundnumber,
                   transaction_date,
                   cast('{cycle_date}' as date) as original_cycle_date,
                   cast('{batchid}' as int) as original_batch_id,
                   'current' as load_type,
                   findw_include_exclude
                   (line_amount)
            from {findw_source_view}
            union all
            select activitytypegroup,
                   activitytype,
                   activityamounttype,
                   fundclassindicator,
                   contractadministrationlocationcode,
                   contractnumber,
                   (source_system_ovrd),
                   ifrs17grouping,
                   ifrs17portfolio,
                   ifrs17profitability,
                   ifrs17cohort,
                   plancode,
                   ledger_name,
                   gl_application_area_code,
                   gl_source_code,
                   fundnumber,
                   cast(transaction_date as date) as transaction_date,
                   cast(original_cycle_date as date) as original_cycle_date,
                   cast(original_batch_id as int) as original_batch_id,
                   'Error' as load_type,
                   'include' as findw_include_exclude
                   (line_error_amount)
            from {error_view_name}
         )
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
             18,
             19,
             20
),
activity_keys as (
    select *,
           sha2(
               concat_ws('|',
                         nvl(activitytypegroup,''),
                         nvl(activitytype,''),
                         nvl(activityamounttype,''),
                         nvl(fundclassindicator,''),
                         nvl(contractnumber,''),
                         nvl(source_system_ovrd_sum,''),
                         nvl(ifrs17grouping,''),
                         nvl(ifrs17portfolio,''),
                         nvl(ifrs17profitability,''),
                         nvl(ifrs17cohort,''),
                         nvl(plancode,''),
                         nvl(ledger_name,''),
                         nvl(gl_application_area_code,''),
                         nvl(gl_source_code,''),
                         nvl(transaction_date,''),
                         findw_include_exclude,
                         original_cycle_date,
                         original_batch_id),
               256
           ) as activity_key
    from source
),
fund_lookups as (
    select activity_key,
           fundnumber_array,
           array_join(collect_list(case when f.fund_nbr is null then exploded.fund_nbr end),',') as missing_fund_numbers,
           collect_list(f.fund_clss_ind) as fundclassindicator_array
    from (
            select activity_key,
                   source_system_nm,
                   contractadministrationlocationcode,
                   fundnumber_array,
                   explode(fundnumber_array) as fund_nbr
            from activity_keys
            where source_system_nm like 'VantageP%'
              and findw_include_exclude = 'include'
         ) exploded
    left join lkp_fnd_clss_ind f
           on f.src_sys_nm = exploded.source_system_nm
          and exploded.contractadministrationlocationcode = f.cntrt_adm_loc_cd
          and exploded.fund_nbr = f.fund_nbr
    group by 1, 2
),
master as (
    select a.*,
           b.evnt_typ_cd as event_type_code_drvd,
           d.oracle_fah_sublgdr_nm as subledger_short_name_drvd,
           e.fah_ldgr_nm as ledger_name_drvd,
           b.trnsctn_clss as transactionclass_drvd,
           b.trnsctn_rdr_typ as transactionordertypedrvd,
           b.trnsctn_fee_typ as transactionfeetype_drvd,
           b.trnsctn_chrg_typ as transactionchargetype_drvd,
           b.trnsctn_tax_typ as transactiontaxtype_drvd,
           case
               when nvl(a.ifrs17grouping,'@') = '@'
                    and a.contractnumber is not null
                    and a.findw_include_exclude = 'include'
                    then c.ifrs17_grpng
           end as ifrs17grouping_drvd,
           case
               when nvl(a.ifrs17portfolio,'@') = '@'
                    and a.contractnumber is not null
                    and a.findw_include_exclude = 'include'
                    then nvl(c.ifrs17_prtfolio,'@')
           end as ifrs17portfolio_drvd,
           case
               when nvl(a.ifrs17cohort,'@') = '@'
                    and a.contractnumber is not null
                    and a.findw_include_exclude = 'include'
                    then c.ifrs17_cohort
           end as ifrs17cohort_drvd,
           case
               when nvl(a.ifrs17profitability,'@') = '@'
                    and a.contractnumber is not null
                    and a.findw_include_exclude = 'include'
                    then c.ifrs17_prftblty
           end as ifrs17profitability_drvd,
           c.ifrs17_msrmnt_mdl as ifrs17measurementmodel_drvd,
           null as ifrs17reportingcashflowtype_drvd,
           case
               when a.source_system_nm like 'VantageP%'
                    and a.findw_include_exclude = 'include'
                    then fl.fundclassindicator_array
           end as fundclassindicator_drvd,
           a.activity_key,
           sha2(
    concat_ws(
        '|',
        nvl(a.fundclassindicator, ''),
        nvl(a.source_system_nm, ''),
        nvl(a.plancode, ''),
        nvl(ifrs17grouping_drvd, ''),
        nvl(ifrs17portfolio_drvd, ''),
        nvl(event_type_code_drvd, ''),
        nvl(transactionridertype_drvd, ''),
        nvl(transactionchargetype_drvd, ''),
        nvl(transactionfeetype_drvd, ''),
        nvl(transactionclass_drvd, '')
    ),
    256
) as lkup_key,
trim(
    case
        when a.contractnumber is not null
             and a.findw_include_exclude = 'include' then
            case
                when event_type_code_drvd is null then 'event_type_code is missing in RDM table lkp_findw_bus_evnt_typ for key combination activitytypegroup (' || nvl(a.activitytypegroup, '') || '), activityamounttype (' || nvl(a.activityamounttype, '') || ') and activitytype (' || nvl(a.activitytype, '') || ')'
                else ''
            end
            || case
                   when ledger_name_drvd is null then 'ledger_name is missing in RDM table lkp_bus_evnt_ldgr_nm for key combination source_system_nm (' || nvl(source_system_nm, '') || ') and activitysourcelegalentitycode (' || nvl(ledger_name, '') || ')'
                   else ''
               end
            || case
                   when subledger_short_name_drvd is null then 'subledger_short_name is missing in RDM table lkp_actvty_gl_sblggr_nm for key combination activitygeneralglapplicationareacode (' || nvl(a.gl_application_area_code, '') || ') and activitygeneralglsourcecode (' || nvl(gl_source_code, '') || ')'
                   else ''
               end
        else ''
    end
) as header_error_message_,
case
    when a.source_system_nm like 'VantageP%'
         and a.findw_include_exclude = 'include'
         and fl.missing_fund_numbers is not null
         and fl.missing_fund_numbers != '' then 'fundclassindicator is missing in RDM table lkp_fnd_clss_ind for key combination contractsourcesystemname (' || nvl(a.source_system_nm, '') || '), contractadministrationlocationcode (' || nvl(a.contractadministrationlocationcode, '') || ') and fundnumber (' || fl.missing_fund_numbers || ')'
    else ''
end as header_error_message_2,
trim(
    case
        when header_error_message_ is not null then 'Header Errored for RDM Lookups;'
        else ''
    end
    ||
    case
        when a.findw_include_exclude = 'include' then
            case when transactionclass_drvd is null
                      or transactionridertype_drvd is null
                      or transactionchargetype_drvd is null
                      or transactionfeetype_drvd is null
                      or transactiontaxtype_drvd is null then 'transactionclass/transactionridertype/transactionchargetype/transactionfeetype/transactiontaxtype is missing in RDM table lkp_findw_bus_evnt_typ for key combination activitytypegroup (' || nvl(a.activitytypegroup, '') || ') , activityamounttype (' || nvl(a.activityamounttype, '') || ') and activitytype (' || nvl(a.activitytype, '') || ')'
                 else ''
            end
        else ''
    end
) as header_error_message,
case
    when a.contractnumber is not null then
        case when ifrs17grouping_drvd is null then 'ifrs17grouping is missing in RDM table lkp_ifrs17_cntrt_tag_vw for key combination contractnumber and sourcesystem;'
             else ''
        end
        || case when ifrs17portfolio_drvd is null then 'ifrs17portfolio is missing in RDM table lkp_ifrs17_cntrt_tag_vw for key combination contractnumber and sourcesystem;'
                else ''
           end
        || case when ifrs17cohort_drvd is null then 'ifrs17cohort is missing in RDM table lkp_ifrs17_cntrt_tag_vw for key combination contractnumber and sourcesystem;'
                else ''
           end
        || case when ifrs17profitability_drvd is null then 'ifrs17profitability is missing in RDM table lkp_ifrs17_cntrt_tag_vw for key combination contractnumber and sourcesystem;'
                else ''
           end
        || case when ifrs17measurementmodel_drvd is null then 'ifrs17measurementmodel is missing in RDM table lkp_ifrs17_cntrt_tag_vw for key combination contractnumber and sourcesystem;'
                else ''
           end
    else ''
end as line_error_message_,
decode(
    trim(line_error_message_),
    '',
    null,
    trim(line_error_message_)
) as line_error_message,
decode(
    trim(header_error_message_ || ' ' || line_error_message_),
    '',
    null,
    trim(header_error_message_ || ' ' || line_error_message_)
) as error_message
from activity_keys a
left join lkp_findw_bus_evnt_typ b on b.actvty_typ_grp = a.activitytypegroup
    and b.actvty_typ = a.activitytype
    and b.actvty_amt_typ = a.activityamounttype
    and a.findw_include_exclude = 'include'
left join lkp_actvty_gl_sblggr_nm d on a.gl_application_area_code = d.actvty_gl_app_area_cd
    and a.gl_source_code = d.actvty_gl_src_cd
    and a.findw_include_exclude = 'include'
left join lkp_bus_evnt_ldgr_nm e on e.src_sys_nm = a.source_system_nm
    and e.src_lgl_enty = a.ledger_name
    and a.findw_include_exclude = 'include'
left join lkp_ifrs17_cntrt_tag_vw c on c.origntng_cntrct_nbr = a.contractnumber
    and c.origntng_src_sys_nm = a.source_system_nm
    and a.contractnumber is not null
    and a.findw_include_exclude = 'include'
left join fund_lookups fl on fl.activity_key = a.activity_key
select *
from master