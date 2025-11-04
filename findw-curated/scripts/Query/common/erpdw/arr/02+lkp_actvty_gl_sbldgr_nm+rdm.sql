select evnt_typ_cd,
src_sys_nm_desc,
initcap(act_src_sys_nm) as act_src_sys_nm,
actvty_gl_app_area_cd,
actvty_gl_src_cd,
oracle_fah_sbldgr_nm,
include_exclude
from time.sch.lkp_actvty_gl_sbldgr_nm
where {other_disable}
to_date('{cycledate}','yyyymmdd') between eff_strt_dt and eff_stop_dt
and actvty_gl_app_area_cd = '~'
and actvty_gl_src_cd = '~'
