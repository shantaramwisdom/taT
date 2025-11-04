select evnt_typ_cd,  -- Event type code
       src_sys_nm_desc,  -- Source system name description
       actvty_gl_app_area_cd,  -- Activity GL application area code
       actvty_gl_src_cd,  -- Activity GL source code
       oracle_fah_subldr_nm,  -- Oracle FAH subledger name
       include_exclude_actvty_gl_sbdgr_nm  -- Include/exclude activity GL subledger name
from time_sch_lkp_actvty_gl_sbdgr_nm  -- Lookup table source
where to_date('{cwlendate}', 'yyyymmdd')  -- Convert control window end date
      between eff_strt_dt and eff_stop_dt;  -- Check effective start and stop date range
