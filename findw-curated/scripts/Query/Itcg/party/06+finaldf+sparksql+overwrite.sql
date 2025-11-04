select
from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') AS recorded_timestamp,
'{source_system_name}' AS source_system_name,
SHA2(concat(COALESCE(concat('LTCG:', life.life_id), ''), 'LTCG'),256) as documentid,
concat('LTCG:',life.life_id) as sourcepartyid,
'LTCG' as sourcesystemname,
coalesce(LKP_CNTRT_ADMN_LOC_CD.cntrt_admn_loc_cd, '@') as contractadministrationlocationcode,
life.life_fname as partyfirstname,
life.life_lname as partylastname,
life.life_mname as partymiddlename,
case
when international_address_flg = '0' then life_address1
when international_address_flg = '1' then intl_line_1
else null
end as partyprimaryaddressline1,
case
when international_address_flg = '0' then life_address2
when international_address_flg = '1' then intl_line_2
else null
end as partyprimaryaddressline2,
null as partyprimaryaddressline3,
trim(case
when international_address_flg = '0' then life_city
when international_address_flg = '1'
and intl_style_cd = '1' then CONCAT(CONCAT(CONCAT(case when life.intl_line_3 is not null then CONCAT(life.intl_line_3,' ') else '' end),
case when life.intl_line_4 is not null then CONCAT(life.intl_line_4,' ') else '' end),
case when life.intl_line_5 is not null then CONCAT(life.intl_line_5,' ') else '' end), case when life.intl_line_6 is not null
then CONCAT(life.intl_line_6,' ') else '' end, case when life.intl_line_7 is not null then CONCAT(life.intl_line_7,' ') else '' end)
when International_Address_Flg = '1'
and intl_style_cd = '2' then CONCAT(case when life.intl_line_3 is not null then CONCAT(life.intl_line_3,' ') else '' end,
case when life.intl_line_4 is not null then CONCAT(life.intl_line_4,' ') else '' end)
when international_address_flg = '1'
and intl_style_cd = '3' then CONCAT(CONCAT(case when life.intl_line_3 is not null then CONCAT(life.intl_line_3,' ') else '' end,
case when life.intl_line_4 is not null then CONCAT(life.intl_line_4,' ') else '' end), case when life.intl_line_5 is not null
then CONCAT(life.intl_line_5,' ') else '' end)
else null
end) as partyprimarycityname,
case
when life.International_Address_Flg = '0' then dim_ggrphy_local_state.iso_state_prvnc_cd
when life.International_Address_Flg = '1' then '#'
else '0'
end as partylocationstatecode,
case
when international_address_flg = '0' then life_zip
when international_address_flg = '1'
and intl_style_cd = '1' then life.intl_line_6
when international_address_flg = '1'
and intl_style_cd = '2' then life.intl_line_6
when international_address_flg = '1'
and intl_style_cd = '3' then life.intl_line_7
else ''
end as partyprimarypostalcode,
case
when life.International_Address_Flg = '0'
and sysnm.cntrt_src_sys_nm = ggphy_local_state.SRC_SYS_NM
and life.life_state = ggphy_local_state.SRC_input
and ggphy_local_state.SRC_INPUT_TYPE = 'State'
and ggphy_local_state.GGRPHY_ID = dim_ggphy_local_state.GGRPHY_ID then dim_ggphy_local_state.ISO_CNTRY_CD
when International_Address_Flg = '1'
and intl_style_cd = '1' then 'US'
when International_Address_Flg = '1'
and intl_style_cd = '2' then 'CA'
when International_Address_Flg = '1'
and intl_style_cd = '3'
and sysnm.cntrt_src_sys_nm = ggphy_local_state.SRC_SYS_NM
and ggphy_local_state.SRC_INPUT = life.INTL_LINE_6
and ggphy_local_state.SRC_INPUT_TYPE = 'Country'
and ggphy_local_state.GGRPHY_ID = dim_ggphy_local_State.GGRPHY_ID then dim_ggphy_local_State.ISO_CNTRY_CD
else ''
end as primarycountryname,
life.contractnumber,
CAST('{cycle_date}' AS date) AS cycle_date,
CAST({batchid} AS int) AS batch_id
from
party_src life
left join LKP_CNTRT_SRC_SYS_NM sysnm on
sysnm.cntrt_src_sys_nm_src = 'LTCG'
left join LKP_CNTRT_ADMN_LOC_CD on
admin_prcss_leg_enty_cd_src = cntrt_src_sys_nm_src
left join LKP_SRC_GGRPHY ggphy_local_state on
sysnm.cntrt_src_sys_nm = ggphy_local_state.SRC_SYS_NM
and life.life_state = ggphy_local_state.SRC_input
and ggphy_local_state.SRC_INPUT_TYPE = 'State'
left join DIM_GGRPHY dim_ggphy_local_State on
ggphy_local_state.GGRPHY_ID = dim_ggphy_local_State.GGRPHY_ID