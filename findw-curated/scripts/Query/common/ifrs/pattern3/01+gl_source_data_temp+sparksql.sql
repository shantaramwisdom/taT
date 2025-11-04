select a.*,
       b.origntng_cntrct_nbr,
       b.origntng_src_sys_nm,
       b.ifrs17_cohort,
       b.ifrs17_grpng,
       b.ifrs17_msrmnt_mdl,
       b.ifrs17_prtfolio,
       b.center_block,
       b.ifrs17_prftblty,
       case
           when b.origntng_cntrct_nbr is not null then b.origntng_src_sys_nm
       end as contractsourcesystemname_ifrs,
       case when '{ifrs17_pattern2}' = 'Y' then c.lgl_enty_cd || '01'
            else a.orig_gl_company
       end as orig_gl_company_ifrs
from gl_source_data a
--step 1 (Fail go to Step 2, Success Step 4)
/*
Lkp time.sch.LKP_IFRS17_CNTRT_TAG_VW (TAG_VW) where TAG_VW.ORIGNTNG_SRC_SYS_NM in ('LifePro109')
and TAG_VW.ORIGNTNG_CNTRCT_NBR = general_Ledger_Line_item.CONTRACTNUMBER then
assign IFRS17_PORTFOLIO, IFRS17_GROUPING, IFRS17_COHORT, IFRS17_PROFITABILITY, IFRS17_MEASUREMENT_MODEL, CENTER_BLOCK and move to CDE IFRS17CASHFLOWTYPE for Step 4, 5 & 6 to assign IFRS17CASHFLOWTYPE
If not found in LKP_IFRS17_CNTRT_TAG_VW table, then Step 2
*/
left join lkp_ifrs17_cntrt_tag_vw b on a.contractnumber = b.origntng_cntrct_nbr --and 1 = 2
left join lkp_lgl_enty_cd c on '{ifrs17_pattern2}' = 'Y' and a.orig_gl_company = c.src_lgl_enty_cd