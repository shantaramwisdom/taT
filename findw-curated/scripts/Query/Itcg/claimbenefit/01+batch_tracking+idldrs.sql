-- 01+batch_tracking+idldrs.sql
SELECT
    min(cycle_date) as min_cycle_date,
    ssm.source_system_name as source_system_name
FROM
    ingestion.batch_tracking bt
inner join preingestion.source_system_master ssm
    on bt.source_system_id = ssm.source_system_id
WHERE
    ssm.source_system_name = 'LTCG'
AND
    bt.status = 'SUCCESS'
group by
    ssm.source_system_name