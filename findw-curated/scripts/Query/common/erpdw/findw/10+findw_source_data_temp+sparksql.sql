select *
from findw_activity_error_lkp
/*
--Enable with Actual Column to ensure the UNION ALL is valid
select *
from findw_source_data_temp
union all
select *
from findw_activity_error_lkp
where contractnumber is not null */