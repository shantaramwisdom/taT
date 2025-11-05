select cntrt_src_sys_nm,
       src_lgl_enty_cd,
       lgl_enty_cd
from time_sch.lkp_lgl_enty_cd
where to_date('{cycledate}', 'yyyymmdd') between eff_strt_dt and eff_stop_dt
