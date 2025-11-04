select
{source_system_name} as source_system_name,
sha2(
concat(
contractnumber, ':', coalesce(
lkp_cntrt_admn_loc_cd_contract.cntrt_admn_loc_cd,
'@'
), ':', 'LTCG'
),
256
) as documentid,
'LTCG' as sourcesystemname,
coalesce(
lkp_cntrt_admn_loc_cd_contract.cntrt_admn_loc_cd,
'@'
) as contractadministrationlocationcode,
contractnumber,
'N' as CONTRACTSOPSINDICATOR,
contracteffectivedate,
contractissuedate,
contractnumber as originatingcontractnumber,
coalesce(
lkp_sys_nm_contract.CNTRT_SRC_SYS_NM_SRC,
'@'
) as ORIGINATINGSOURCESYSTEMNAME,
coalesce(
lkp_cntrt_prtcpn_ind.cntrt_prtcpn_ind,
'@'
) as contractparticipationindicator,
case when contract_src.reporting_lob is null then 'S' else coalesce(
lkp_cntrt_grp_ind.cntrt_grp_ind,
'@'
)
end as contractgroupindicator,
case when lkp_ifrs17_cntrt_tag_vw.ifrs17_cohort is not null then lkp_ifrs17_cntrt_tag_vw.ifrs17_cohort when lkp_ifrs17_cntrt_tag_vw.ifrs17_cohort is null
and
(
contractissuedate = '9999-12-31 00:00:00'
and contracteffectivedate < '2022-01-01'
)
or
(
contractissuedate <> '9999-12-31 00:00:00'
and greatest(
contracteffectivedate, contractissuedate
) >= '2022-01-01'
)
then 'e' when
(
contractissuedate = '9999-12-31 00:00:00'
and contracteffectivedate >= '2022-01-01'
)
or
(
contractissuedate <> '9999-12-31 00:00:00'
and greatest(
contracteffectivedate, contractissuedate
) < '2022-01-01'
)
then 'o' end as contractifrs17cohort,
case when lkp_ifrs17_nb_prt_chrt.ifrs17cohort is not null then lkp_ifrs17_nb_prt_chrt.ifrs17cohort else '@' end as contractifrs17cohort,
case when lkp_ifrs17_cntrt_tag_vw.ifrs17_grpng is not null then lkp_ifrs17_cntrt_tag_vw.ifrs17_grpng when greatest(
contracteffectivedate, contractissuedate
) >= '2022-01-01' then 'ltc_open' end as contractifrs17grouping,
case when lkp_ifrs17_cntrt_tag_vw.ifrs17_prtfolio is not null then lkp_ifrs17_cntrt_tag_vw.ifrs17_prtfolio when greatest(
contracteffectivedate, contractissuedate
) < '2022-01-01' then 'ltc' end as contractifrs17portfolio,
case when lkp_ifrs17_cntrt_tag_vw.ifrs17_prftblty is not null then lkp_ifrs17_cntrt_tag_vw.ifrs17_prftblty when greatest(
contracteffectivedate, contractissuedate
) >= '2022-01-01' then 'onerous' end as contractifrs17profitability,
coalesce(
case when BG1_GML_LEVEL is null
and BG1_GRP_REFERENCE is null then 'S' when BG1_GML_LEVEL = '1' then BG1_GRP_REFERENCE else LKP_CNTRT_MKTG_ORG_CD.CNTRT_SRC_MKT_ORG_CD end,
'@'
) as contractmarketingorganizationcode,
coalesce(
lkp_cntrt_prdct_typ_nm.cntrt_prdct_typ_nm,
'@'
) as contractproducttypename,
case when tax_qualif_grandfathered_flg = 'y' then 'q' else lkp_cntrt_qlfd_ind.cntrt_qlfd_ind end as contractualifiedindicator,
case when contract_src.reporting_lob is null then 'S' else contract_src.reporting_lob end as contractreportinglineofbusiness,
coalesce(
dim_cntrt_sts.cntrt_opt_cd,
'@'
) as contractstatuscode,
coalesce(
lkp_src_cntrt_sts.cntrt_opt_sts_rsn_cd,
'@'
) as contractstatusreasoncode,
case when admin_system_plan_cd is null then 'S' else admin_system_plan_cd end as contractplancode,
lkp_stat_cmpy_trnslatn.src_lgl_enty as sourcelegalentitycode,
lkp_lgl_enty_cd.lgl_enty_cd,
'@'
) as contractlegalcompanycode,
annual_premium_amt_sum as contractannualizedpremium,
coalesce(
lkp_cntrt_bill_mod.cntrt_bill_mod,
'@'
) as contractbillingmode,
contract_src.coverage_id as contractcoverageid,
coalesce(
dim_ggrphy.iso_state_prvnc_cd,
'@'
) as contractissuestatecode,
case when frequency_value is null
or lkp_cntrt_admin_prem_md_cd.cntrt_prem_md_cd is null then 0.00 else cast(
coalesce(annual_premium_amt_sum, 0.00) / lkp_cntrt_admin_prem_md_cd.cntrt_prem_md_cd as decimal(18,2)
)
end as contractmodalpremium,
case when partnership_status_cd is null then 'S' else coalesce(
lkp_cntrt_prtnrshp_sts_cd.cntrt_prtnrshp_sts_cd,
'@'
)
end as contractpartnershipstatuscode,
coalesce(
lkp_pay_map.cntrt_pymt_typ,
'@'
) as contractpaymentmethod,
case when frequency_value is null then null else coalesce(
lkp_cntrt_admin_prem_md_cd.cntrt_prem_md_cd,
'@'
)
end as contractpremiummodecode,
cast({cycle_date} as date) as cycle_date,
cast({batchid} as int) as batch_id,
-- IFRS17 CLOUD INSERT FLAG
case when LKP_IFRS17_CNTRT_TAG_VW.ORIGNTNG_SRC_SYS_NM IS NULL
and LKP_IFRS17_CNTRT_TAG_VW.ORIGNTNG_CNTRT_NBR IS NULL
and LKP_IFRS17_NB_PRT_CHRT.IFRS17COHORT IS NOT NULL
and
(
contractissuedate = '9999-12-31 00:00:00'
and contracteffectivedate >= '2022-01-01'
)
or
(
contractissuedate <> '9999-12-31 00:00:00'
and greatest(
contracteffectivedate, contractissuedate
) >= '2022-01-01'
)
then 'INSERT' else null end as rdm_insert_flag
from contract_src
left join lkp_cntrt_prtcpn_ind on lkp_cntrt_prtcpn_ind.CNTRT_SRC_SYS_NM = 'LTCG'
left join lkp_sys_nm_contract on lkp_sys_nm_contract.CNTRT_SRC_SYS_NM_SRC = 'LTCG'
left join lkp_cntrt_admn_loc_cd_contract on lkp_cntrt_admn_loc_cd_contract.CNTRT_SRC_SYS_NM_SRC = 'LTCG'
left join lkp_cntrt_grp_ind on lkp_cntrt_grp_ind.cntrt_grp_ln = contract_src.reporting_lob
left join lkp_cntrt_prdct_typ_nm
left join lkp_cntrt_qlfd_ind on contract_src.tax_qualification_cd = lkp_cntrt_qlfd_ind.src_cntrt_qlfd_ind
and lkp_cntrt_qlfd_ind.eff_strt_dt <= contract_src.cycle_date
and contract_src.cycle_date <= lkp_cntrt_qlfd_ind.eff_stop_dt
left join lkp_src_cntrt_sts on contract_src.policy_status_cd = lkp_src_cntrt_sts.src_input
left join dim_cntrt_sts on coalesce(
lkp_src_cntrt_sts.cntrt_opt_sts_rsn_cd,
'@'
) = dim_cntrt_sts.cntrt_opt_sts_rsn_cd
left join lkp_stat_cmpy_trnslatn on left(contract_src.contractnumber, 2) = lkp_stat_cmpy_trnslatn.origntng_lgl_enty
and lkp_stat_cmpy_trnslatn.eff_strt_dt <= contract_src.cycle_date
and contract_src.cycle_date <= lkp_stat_cmpy_trnslatn.eff_stop_dt
left join lkp_lgl_enty_cd on lkp_lgl_enty_cd.src_lgl_enty_cd = coalesce(
lkp_stat_cmpy_trnslatn.src_lgl_enty,
'@'
)
and lkp_lgl_enty_cd.eff_strt_dt <= contract_src.cycle_date
and contract_src.cycle_date <= lkp_lgl_enty_cd.eff_stop_dt
left join lkp_cntrt_mktg_org_cd on grp_reference = lkp_cntrt_mktg_org_cd.mkt_org_raw
left join lkp_ifrs17_cntrt_tag_vw on originating_cntrct_nbr = contractnumber
left join lkp_ifrs17_nb_prt_chrt on (
case when contractissuedate like '9999-12-31%' then contracteffectivedate else greatest(
contractissuedate, contracteffectivedate
)
end
) >= ifrs17_cohort_strt_dt
and (
case when contractissuedate like '9999-12-31%' then contracteffectivedate else greatest(
contractissuedate, contracteffectivedate
)
end
) <= ifrs17_cohort_end_dt
left join lkp_cntrt_bill_mod on lkp_cntrt_bill_mod.src_cntrt_bill_mod = contract_src.frequency_value
left join lkp_cntrt_admin_prem_md_cd on lkp_cntrt_admin_prem_md_cd.cntrt_admin_prem_md_cd = contract_src.frequency_value
left join lkp_pay_map on contract_src.billing_type_cd = lkp_pay_map.fbform_form
left join lkp_cntrt_prtnrshp_sts_cd on contract_src.partnership_status_cd = lkp_cntrt_prtnrshp_sts_cd.cntrt_prtnrshp_sts_cd_raw
left join lkp_src_ggrphy on lkp_src_ggrphy.src_input = contract_src.filing_state
left join dim_ggrphy on dim_ggrphy.ggrphy_id = lkp_src_ggrphy.ggrphy_id