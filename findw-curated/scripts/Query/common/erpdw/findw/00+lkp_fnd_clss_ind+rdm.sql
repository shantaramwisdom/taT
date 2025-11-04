select src_sys_nm,
cntrt_adm_loc_cd,
fund_nbr,
fund_clss_ind,
fund_clss_desc
from time_sch.lkp_fnd_clss_ind
where src_sys_nm = '{findw_originating_systems}'

