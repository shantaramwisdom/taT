select actvty_gl_ldgr_cd,  -- Activity GL ledger code
       oracle_fah_ldgr_nm,  -- Oracle FAH ledger name
       actvty_gl_entty_cd,  -- Activity GL entity code
       actvty_cd || actvty_gl_ldgr_cd as src_ldgr_cd,  -- Concatenate activity code with ledger code
       include_exclude_actvty_ldgr_nm  -- Include/exclude activity ledger name
from time_sch_lkp_actvty_ldgr_nm  -- Lookup table source
where to_date('{cwlendate}', 'yyyymmdd')  -- Convert control window end date
      between eff_strt_dt and eff_stop_dt;  -- Check effective start and stop date range
