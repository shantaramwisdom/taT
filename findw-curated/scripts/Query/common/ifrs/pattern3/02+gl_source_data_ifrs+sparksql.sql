with
base_query as
(select b.*,
            case when b.origntng_cntrct_nbr is not null then b.ifrs17_cohort
            when c.original_clac is not null then c.ifrs17_cohort
            when d.ifrs4_center is not null then d.ifrs17_cohort
            else '@'
            end as ifrs17cohort,
            case when b.origntng_cntrct_nbr is not null then b.ifrs17_grpng
            when c.original_clac is not null then c.ifrs17_grouping
            when d.ifrs4_center is not null then d.ifrs17_grouping
            else '@'
            end as ifrs17grouping,
            case when b.origntng_cntrct_nbr is not null then b.ifrs17_msrmnt_mdl
            when c.original_clac is not null then c.ifrs17_measurement_model
            when d.ifrs4_center is not null then d.ifrs17_measurement_model
            else '@'
            end as ifrs17measurementmodel,
            case when b.origntng_cntrct_nbr is not null then b.ifrs17_prtfolio
            when c.original_clac is not null then c.ifrs17_portfolio
            when d.ifrs4_center is not null then d.ifrs17_portfolio
            else '@'
            end as ifrs17portfolio,
            case when b.origntng_cntrct_nbr is not null then b.ifrs17_prftblty
            when c.original_clac is not null then c.ifrs17_profitability
            when d.ifrs4_center is not null then d.ifrs17_profitability
            else '@'
            end as ifrs17profitability,
            case when b.origntng_cntrct_nbr is not null then b.center_block
            when c.original_clac is not null then c.center_block_foractmapping
            when d.ifrs4_center is not null then d.center_block_foractmapping
            else '@'
            end as centerblockifrs17,
            case when b.origntng_cntrct_nbr is null and c.original_clac is not null then c.cashflow_type
            when b.origntng_cntrct_nbr is null and c.original_clac is null and d.ifrs4_center is null then '0'
            when (b.origntng_cntrct_nbr is not null or d.ifrs4_center is not null)
            and nvl(i.oracle_company,j.oracle_company) is null then '0'
            when l.legal_entity is not null then 'NONE'
            when (i.oracle_company is not null or j.oracle_company is not null)
            and k.name is null and l.legal_entity is null then 'NONE'
            when 'Y' = '{ifrs17_pattern1}' and f.ifrs4_account is not null then g.cashflow_type
            when n1.src_sys_nm is not null then n1.ifrs17_rptng_csh_flw_typ --(from Step 6A) 1st Lookup
            when n2.src_sys_nm is not null then n2.ifrs17_rptng_csh_flw_typ --(from Step 6A) 2nd Lookup
            when n3.src_sys_nm is not null then n3.ifrs17_rptng_csh_flw_typ --(from Step 6A) 3rd Lookup
            when left(b.orig_gl_account,1) in ('1','2','3','9') then 'NONE' --(from Step 6B SKIP)
            when o.account_number is not null then o.reporting_cashflowtype --(from Step 6B)
            end as ifrs17reportingcashflowtype,
            case when b.origntng_cntrct_nbr is null and c.original_clac is not null then
            case when ifrs17measurementmodel like '%SIMP%' then 'N'
            when l.legal_entity is not null then 'N'
            when (i.oracle_company is not null or j.oracle_company is not null)
            and k.name is null and l.legal_entity is null then 'N'
            when 'Y' = '{ifrs17_pattern1}' and f.ifrs4_account is not null then g.send_to_ice
            when ifrs17measurementmodel like '%SIMP%' then 'N'
            else 'Y'
            end
            end as iceinputflag,
            case when b.origntng_cntrct_nbr is not null then 'Step 1'
            when c.original_clac is not null then 'Step 2'
            when d.ifrs4_center is not null then 'Step 3'
            when d.ifrs4_center is null then 'Step 3 Failed (Error)'
            else 'UNKNOWN'
            end as ifrs17_fields_step_dsc,
            case when b.origntng_cntrct_nbr is null and c.original_clac is not null then 'Step 2'
            when b.origntng_cntrct_nbr is null and c.original_clac is null and d.ifrs4_center is null then 'Step 3'
            when (b.origntng_cntrct_nbr is not null or d.ifrs4_center is not null)
            and nvl(i.oracle_company,j.oracle_company) is null then 'Step 4A Failed (Error)'
            when l.legal_entity is not null then 'Step 4C'
            when (i.oracle_company is not null or j.oracle_company is not null)
            and k.name is null and l.legal_entity is null then 'Step 4D'
            when 'Y' = '{ifrs17_pattern1}' and f.ifrs4_account is not null and g.ifrs4_account is null then 'Step 5B Failed (Error)' || decode('{ifrs17_pattern1}','Y',' Pattern 1',' Pattern 2')
            when 'Y' = '{ifrs17_pattern1}' and g.ifrs4_account is not null then 'Step 5B' || decode('{ifrs17_pattern1}','Y',' Pattern 1',' Pattern 2')
            when n1.src_sys_nm is not null then 'Step 6A 1st Lookup' || decode('{ifrs17_pattern1}','Y',' Pattern 1',' Pattern 2')
            when n2.src_sys_nm is not null then 'Step 6A 2nd Lookup' || decode('{ifrs17_pattern1}','Y',' Pattern 1',' Pattern 2')
            when n3.src_sys_nm is not null then 'Step 6A 3rd Lookup' || decode('{ifrs17_pattern1}','Y',' Pattern 1',' Pattern 2')
            when left(b.orig_gl_account,1) in ('1','2','3','9') then 'Step 6B SKIP'
            when o.account_number is not null then 'Step 6B'
            else 'UNKNOWN'
            end as ifrs17_cash_flow_step_dsc,
            case when ifrs17cohort = '0' or ifrs17grouping = '0' or ifrs17measurementmodel = '0' or ifrs17portfolio = '0' or ifrs17profitability = '0' or ifrs17reportingcashflowtype = '0'
            then
            CONCAT_WS(',', 
            case when ifrs17cohort = '0' then ifrs17cohort end,
            case when ifrs17grouping = '0' then ifrs17grouping end,
            case when ifrs17measurementmodel = '0' then ifrs17measurementmodel end,
            case when ifrs17portfolio = '0' then ifrs17portfolio end,
            case when ifrs17profitability = '0' then ifrs17profitability end,
            case when ifrs17reportingcashflowtype = '0' then ifrs17reportingcashflowtype end
            ) || ' Lookup is Invalid for Contract ' || nvl(contractnumber,'NULL') || ' and Original CLAC ' ||
            nvl(b.orig_gl_company_ifrs,'NULL') || '-' || nvl(b.orig_gl_account,'NULL') || '-' || nvl(b.orig_gl_center,'NULL')
            else null
            end as ifrs_error_msg
from
/*
Alias Step Action
a Step 1: gl_source_data_temp aka Contract Tagging View LKP Fail go to Step 2, Success Step 4A
b Step 2: CLAC Override Fail go to Step 3, Else Exit
c Step 3: Center Tagging Fail Error, Success Step 4A
d Step 4A: LE Convert GEAC_Company (LE) to Oracle_Company Fail Error, Success Step 4B
i/j Step 4B: Lkp temp IDL table Company_Management_Hierarchy_Life_Consolidating not ‘Life Consolidating’ but we want to ‘EXCLUDE’
k Step 4C: not ‘Life Consolidating’ but we want to ‘INCLUDE’
l Step 4D: Policy Loan Cash Flow Type Assignment (Optional) Success Exit, Else Step 5A ???
f Step 5A: Policy Loan RA09 CFT Lookup for Policy Loans (Optional) Success Exit, Else Step 5B
g Step 5B: Cash CFT Assignment Using RA05 BLNCSHT TBL (1st Lookup) Success Exit, Else Step 6A
n1 Step 6A: Cash CFT Assignment Using RA05 BLNCSHT TBL (2nd Lookup) Success Exit, Else Step 6B
n2 Step 6B: Cash CFT Assignment Using RA03 Account to Cash Flow Fail Error, Else Exit
*/

gl_source_data_temp b
--step 2 (Fail go to Step 3, Else Exit)

/*
Step 2: CLAC Override
Lkp RDM table RR_RA07_OVRRD_CLAC_MPPNG (RA07) where RA07.ORIGINAL_CLAC =
("general_ledger_line_item.ORIG_GL_COMPANY" + "-" + "general_ledger_line_item.ORIG_GL_ACCOUNT" + "-" + "general_ledger_line_item.ORIG_GL_CENTER")
(example '0701-6215036-7013800') then assign IFRS17_PORTFOLIO, IFRS17_GROUPING, IFRS17_COHORT, IFRS17_PROFITABILITY, IFRS17_MEASUREMENT_MODEL, CENTER_BLOCK & IFRS17CASHFLOWTYPE from RA07.CASHFLOW_TYPE
and if general_ledger_line_item.IFRS17_MEASUREMENT_MODEL Contains 'SIMP'
then assign general_ledger_line_item.ICEInputFlag = 'N' else assign 'Y' and exit
If not found then Step 3
*/

left join rr_ra07_ovrrd_clac_mppng c on b.origntng_cntrct_nbr is null
and c.original_clac = concat(b.orig_gl_company_ifrs,'-', b.orig_gl_account,'-', b.orig_gl_center)
--and 1 = 2
--step 3 (Fail Error, Success Step 4A)

/*
Step 3: Center Tagging
Lkp RDM table RR_RA01_CNTR_TO_CNTR_MPPNG (RA01) where RA01.IFRS4_CENTER = general_ledger_line_item.ORIG_GL_CENTER and RA01.RECLASS_CENTER_IFRS17 does not contain '_NOC' then Assign IFRS17_PORTFOLIO, IFRS17_GROUPING, IFRS17_COHORT, IFRS17_PROFITABILITY, IFRS17_MEASUREMENT_MODEL and CENTER_BLOCK and go to CDE IFRS17CASHFLOWTYPE for Step 4, 5 & 6 to assign IFRS17CASHFLOWTYPE
If not found then assign (‘0’ Not Found) to IFRS17_PORTFOLIO, IFRS17_GROUPING, IFRS17_COHORT, IFRS17CASHFLOWTYPE and send to <Error Handling>
*/

left join rr_ra01_cntr_to_cntr_mppng d on b.origntng_cntrct_nbr is null and c.original_clac is null
and d.ifrs4_center = b.orig_gl_center
and d.reclass_center_ifrs17 not like '%_NOC%'
--and 1 = 2
--step 4A (Fail Error, Success Step 4B)

/*
Step 4A - Legal Entity (LE) Filter - Convert GEAC_Company (LE) to Oracle_Company
Lkp IDL table GEAC_Oracle_Company_Map_Current where GEAC_Oracle_Company_Map_Current.GEAC_COMPANY = left(general_Ledger_Line_item.ORIG_GL_COMPANY, 2)
and GEAC_Oracle_Company_Map_Current.GEAC_CENTER = general_Ledger_Line_item.ORIG_GL_CENTER then store GEAC_Oracle_Company_Map_Current.ORACLE_COMPANY and go to Step 4B
If not found then do a lookup on GEAC_Oracle_Company_Map_Current where GEAC_Oracle_Company_Map_Current.GEAC_COMPANY = left(general_Ledger_Line_item.ORIG_GL_COMPANY, 2)
and GEAC_Oracle_Company_Map_Current.GEAC_CENTER = ‘’ then store GEAC_Oracle_Company_Map_Current.ORACLE_COMPANY and go to Step 4B,
If not found assign ‘0’ (Not Found) to general_Ledger_Line_item.IFRS17REPORTINGCASHFLOWTYPE and send to <Error Handling>
*/

left join (datalake_ref.edmdcs_db).oracle_geac_company_map_current i on (b.origntng_cntrct_nbr is not null or d.ifrs4_center is not null)
and cast('{cycle_date}' as date) between cast(i.eff_start_dt as date) and cast(i.eff_stop_dt as date)
and i.geac_company = left(b.orig_gl_company_ifrs, 2) and i.geac_center = b.orig_gl_center
left join (datalake_ref.edmdcs_db).oracle_geac_company_map_current j on (b.origntng_cntrct_nbr is not null or d.ifrs4_center is not null)
and cast('{cycle_date}' as date) between cast(j.eff_start_dt as date) and cast(j.eff_stop_dt as date)
and j.geac_company = left(b.orig_gl_company_ifrs, 2) and j.geac_center = '*'

/*
--step 4B (Success 4C, Else 4D)
Step 4B - Lkp temp IDL table Company_Management_Hierarchy_Life_Consolidating
where Company_Management_Hierarchy_Life_Consolidating.Name = geac_oracle_company_map_current.ORACLE_COMPANY,
If Found then go to Step 4C If not found then go to Step 4D
*/
        left join company_management_hierarchy_life_consolidating k on (i.oracle_company is not null or j.oracle_company is not null)
        and k.name = nvl(i.oracle_company, j.oracle_company)
        --step 4C (Success Exit, Else 5A)
/*
Step 4C - Filter LE that are not 'Life Consolidating' but we want to 'EXCLUDE'
Lkp RDM table RA05_COMPANY_EXCEPTION (RA05) table where RA05.LEGAL_ENTITY = geac_oracle_company_map_current.ORACLE_COMPANY and RA05.EXCEPTION = 'EXCLUDE'
If found then move 'NONE' to general_ledger_line_item.IFRS17REPORTINGCASHFLOWTYPE, move 'N' to general_ledger_line_item.ICEInputFlag and exit
Else not found go to Step 5A
*/
        left join rr_ra05_company_exception l on k.name is not null and l.legal_entity = k.name and l.exception = 'EXCLUDE'
        --step 4D (Success 5A, Else Exit)
/*
Step 4D - Filter LE that are not 'Life Consolidating' but we want to 'INCLUDE'
Lkp RDM table RA05_COMPANY_EXCEPTION (RA05) table where RA05.LEGAL_ENTITY = geac_oracle_company_map_current.ORACLE_COMPANY and RA05.EXCEPTION = 'INCLUDE'
If found go to Step 5A else if not found move 'NONE' to general_Ledger_Line_item.IFRS17REPORTINGCASHFLOWTYPE, move 'N' general_Ledger_Line_item.ICEInputFlag and exit
*/
        left join rr_ra05_company_exception m on (i.oracle_company is not null or j.oracle_company is not null)
        and k.name is null and l.legal_entity is null and nvl(i.oracle_company, j.oracle_company) = m.legal_entity and m.exception = 'INCLUDE'
        --step 5a (Optional) (Success Step 5B else Step 6A)
/*
Step 5A: Policy Loan Cash Flow Type Assignment - Check to see if Acct Number exist on RA09 by Acct Number only
LKP RDM table RR_RA09_POLICY_LOAN_MPPNG (RA09) WHERE RA09.IFRS4_Account = left(general_Ledger_Line_item.ORIG_GL_ACCOUNT,5)
and RA09.IFRS17_ACCOUNT does not contain '_EXCLUDE'
If Found go to Step 5B Else go to Step 6A
*/
        left join rr_ra09_policy_loan_mppng f on 'Y' = '{ifrs17_pattern1}'
        and (l.legal_entity is not null or m.legal_entity is not null)
        and f.ifrs17_account not like '%_EXCLUDE%'
        and f.ifrs4_account = left(b.orig_gl_account, 5)
        --step 5B (Optional) (Fail Error, Else Exit)
/*
Step 5B: Policy Loan RA09 CFT Lookup for Policy Loans
LKP RDM table RR_RA09_POLICY_LOAN_MPPNG (RA09) WHERE RA09.Contract_Source_System in 'LifePro109'
and RA09.IFRS4_Account = left(general_Ledger_Line_item.ORIG_GL_ACCOUNT,5)
and RA09.Trans_Type_cd = general_Ledger_Line_item.ACTIVITYSOURCETRANSACTIONCODE
and RA09.IFRS17_Portfolio = general_Ledger_Line_item.IFRS17PORTFOLIO and RA09.IFRS17_Grouping = general_Ledger_Line_item.IFRS17GROUPING
and RA09.IFRS17_Account does not contain '_EXCLUDE'
then assign general_Ledger_Line_item.IFRS17REPORTINGCASHFLOWTYPE from RA09.CashFlow_Type
and assign general_Ledger_Line_item.ICEInputFlag from RA09.SEND_TO_ICE and exit.
Else if not found assign '@' (Not Found) to general_Ledger_Line_item.IFRS17REPORTINGCASHFLOWTYPE and send to <Error Handling>
*/
        left join rr_ra09_policy_loan_mppng g on 'Y' = '{ifrs17_pattern1}'
        and f.ifrs4_account is not null
        --and g.contract_source_system in ({ifrs_originating_systems})
        and g.ifrs17_account not like '%_EXCLUDE%'
        and g.ifrs4_account = left(b.orig_gl_account, 5)
        and g.trans_type_cd = {ifrs17_activitysourcetransactioncode_joiner} --pattern1 only enable where applicable
        and g.ifrs17_grouping =
            case when b.origtng_cntrct_nbr is not null then b.ifrs17_grpng
                when c.original_clac is not null then c.ifrs17_grouping
                when d.ifrs4_center is not null then d.ifrs17_grouping
                --else '@'
            end
        and g.ifrs17_portfolio =
            case when b.origtng_cntrct_nbr is not null then b.ifrs17_prtfolio
                when c.original_clac is not null then c.ifrs17_portfolio
                when d.ifrs4_center is not null then d.ifrs17_portfolio
                --else '@'
            end
        --step 6A (Success Exit, Else Step 6B) (Come from Step 1/Step 3 via Step 4 or Step 5a) ??
/*
Step 6A: Cash CFT Assignment Using Balance Sheet Cash Flow Type Table
If general_Ledger_Line_item.IFRS17_MEASUREMENT_MODEL contains 'SIMP' then assign general_Ledger_Line_item.ICEInputFlag = 'N' else assign 'Y'
Lkp RDM table LKP_BLNC_SHT_RPTNG_CSHFLW_TYP where LKP_BLNC_SHT_RPTNG_CSHFLW_TYP.SRC_SYS_NM = general_Ledger_header.SOURCE_SYSTEM_NM
and LKP_BLNC_SHT_RPTNG_CSHFLW_TYP.GL_ACCT_NMBR = left(general_Ledger_Line_item.ORIG_GL_ACCOUNT, 5)
and LKP_BLNC_SHT_RPTNG_CSHFLW_TYP.CNTR_NMBR = general_Ledger_Line_item.ORIG_GL_CENTER,
If not Found 2nd find and Lookup moving to CNTR_NMBR,
If still not found repopulate CNTR_NMBR and move to '0 ACCI_NMBR,
If Found in any of the 3 Lookups Assign IFRS17REPORTINGCASHFLOWTYPE from LKP_BLNC_SHT_RPTNG_CSHFLW_TYP.IFRS17_RPRTING_CSH_FLW_TYP and exit,
If Not Found after all 3 lookups go to Step 6B
*/
        --step 6A (Fail 2nd Lookup, Else Exit) 1st Lookup
        left join lkp_blnc_sht_rptng_cshflw_typ n on
        case when left(b.orig_gl_account, 1) in ('1','2','3','9') then 'N' else 'Y' end = 'Y' and
        case when b.origtng_cntrct_nbr is null and c.original_clac is not null then 'N' else 'Y' end = 'Y' and
        case when l.legal_entity is not null then 'N' else 'Y' end = 'Y' and
        case when 'Y' = '{ifrs17_pattern1}' and f.ifrs4_account is not null then 'N' else 'Y' end = 'Y' and
        n.src_sys_nm in ({ifrs_originating_systems}) and
        n.gl_acct_nmbr = left(b.orig_gl_account, 5) and
        n.cntr_nm = b.orig_gl_center
        --step 6A (Fail 3rd Lookup, Else Exit) 2nd Lookup
        left join lkp_blnc_sht_rptng_cshflw_typ n2 on
        case when left(b.orig_gl_account, 1) in ('1','2','3','9') then 'N' else 'Y' end = 'Y' and
        case when b.origtng_cntrct_nbr is null and c.original_clac is not null then 'N' else 'Y' end = 'Y' and
        case when l.legal_entity is not null then 'N' else 'Y' end = 'Y' and
        case when 'Y' = '{ifrs17_pattern1}' and f.ifrs4_account is not null then 'N' else 'Y' end = 'Y' and
        n.src_sys_nm is null and n2.src_sys_nm in ({ifrs_originating_systems}) and
        n2.gl_acct_nmbr = left(b.orig_gl_account, 5) and
        n2.cntr_nm = '-'
        --step 6A (Fail 6B, Else Exit) 3rd Lookup
        left join lkp_blnc_sht_rptng_cshflw_typ n3 on
        case when left(b.orig_gl_account, 1) in ('1','2','3','9') then 'N' else 'Y' end = 'Y' and
        case when b.origtng_cntrct_nbr is null and c.original_clac is not null then 'N' else 'Y' end = 'Y' and
        case when l.legal_entity is not null then 'N' else 'Y' end = 'Y' and
        case when 'Y' = '{ifrs17_pattern1}' and f.ifrs4_account is not null then 'N' else 'Y' end = 'Y' and
        n.src_sys_nm is null and n2.src_sys_nm is null and
        n3.src_sys_nm in ({ifrs_originating_systems}) and
        n3.gl_acct_nmbr = left(b.orig_gl_account, 5) and
        n3.cntr_nm = b.orig_gl_center
/*
step 6B: Cash CFT Assignment Using RA03 Account to Cash Flow Type Table
If left(general_Ledger_Line_item.ORIG_GL_ACCOUNT,1) in ('1','2','3','9') assign 'NONE' to general_Ledger_Line_item.IFRS17REPORTINGCASHFLOWTYPE and Exit
Else Lkp RDM table RR_RA03_ACCT_TO_CFT_MPPNG (RA03) where RA03.ACCOUNT_NUMBER = Left(general_Ledger_Line_item.ORIG_GL_ACCOUNT, 5)
and RA03.CENTER_BLOCK_FORACCTMAPPING = general_Ledger_Line_item.CENTER_BLOCK then Assign IFRS17REPORTINGCASHFLOWTYPE from RA03.REPORTING_CASHFLOWTYPE and exit.
If Not Found assign '@' (Not Found) to general_Ledger_Line_item.IFRS17REPORTINGCASHFLOWTYPE and send to <Error Handling>
*/
        left join rr_ra03_acct_to_cft_mppng o on
        case when left(b.orig_gl_account, 1) in ('1','2','3','9') then 'N' else 'Y' end = 'Y' and
        case when b.origtng_cntrct_nbr is null and c.original_clac is not null then 'N' else 'Y' end = 'Y' and
        case when l.legal_entity is not null then 'N' else 'Y' end = 'Y' and
        case when 'Y' = '{ifrs17_pattern1}' and f.ifrs4_account is not null then 'N' else 'Y' end = 'Y' and
        n.src_sys_nm is null and n2.src_sys_nm is null and n3.src_sys_nm is null
        and o.account_number = left(b.orig_gl_account, 5)
        and o.center_block_foracctmapping =
            case when b.origtng_cntrct_nbr is not null then b.center_block
                when c.original_clac is not null then c.center_block_foracctmapping
                when d.ifrs4_center is not null then d.center_block_foracctmapping
            end),
header_error_consolidation AS (
    SELECT header_hash,
           concat_ws(';', sort_array(collect_set(NULLIF(ifrs_error_msg, '')))) as header_consolidated_error_msg
    FROM base_query A
    WHERE ifrs_error_msg is not null
    GROUP BY header_hash
),
line_error_consolidation AS (
    SELECT line_hash,
           concat_ws(';', sort_array(collect_set(NULLIF(ifrs_error_msg, '')))) as line_consolidated_error_msg
    FROM base_query
    WHERE 'general_ledger_line_item' = '{domain}'
      AND ifrs_error_msg is not null
    GROUP BY line_hash
)
SELECT b.*,
       TRIM(LEADING ';' from header_consolidated_error_msg) as ifrs_error_message_header,
       TRIM(LEADING ';' from line_consolidated_error_msg) as ifrs_error_message_line
FROM base_query b
LEFT JOIN header_error_consolidation e using (header_hash)
LEFT JOIN line_error_consolidation f using (line_hash);