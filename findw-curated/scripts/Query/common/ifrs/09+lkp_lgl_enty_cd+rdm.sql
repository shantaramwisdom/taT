select cntrct_src_sys_nm,
       src_lgl_enty_cd,
       lgl_enty_cd
from time_sch.lkp_lgl_enty_cd
where to_date('{cycledate}','yyyymmdd') between eff_strt_dt and eff_stop_dt
and cntrct_src_sys_nm in ({ifrs17_lkp_lgl_enty_cd_ss_name})
and '{ifrs17_pattern2}' = 'Y';