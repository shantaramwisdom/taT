SELECT from_utc_timestamp(current_timestamp, 'US/Central') AS recorded_timestamp,
       '{source_system_name}' AS source_system_name,
       src.documentid,
       '{source_system_name}' AS sourcesystemname,
       src.sourcepartyid,
       src.partyfirstname,
       src.partylastname,
       src.partymiddlename,
       COALESCE(LKP_GNDR.STD_GNDR_CD, '@') AS gender,
       CAST(src.dateofbirth AS date) AS dateofbirth,
       src.partyprimaryaddressline1,
       src.partyprimarycityname,
       src.partyprimarypostalcode,
       COALESCE(DIM_GGRPHY.ISO_STATE_PRVNC_CD, '@') AS partylocationstatecode,
       COALESCE(DIM_GGRPHY.ISO_CNTRY_CD, '@') AS primarycountryname,
       CAST('{cycle_date}' AS date) AS cycle_date,
       CAST('{batchid}' AS int) AS batch_id
FROM party_src src
LEFT JOIN LKP_GNDR ON src.sourcesystemname = LKP_GNDR.SRC_SYS_NM
AND lower(src.insured_sex) = LKP_GNDR.GNDR_CD
LEFT JOIN DIM_GGRPHY ON src.insured_state = DIM_GGRPHY.GGRPHY_ID
