select (batchid) batch_id, ('{cycle_date}') cycle_date, ('{domain_name}') domain, ('{source_system_name}') sys_nm, ('{source_system_name}') p_sys_nm, 
'BALANCING_COUNT' measure_name, 'curated' hop_name, 
'recordcount' measure_field, 'INTEGER' as measure_value_datatype, 'S' measure_src_trt_adj_indicator, 
'gdqp5deltaextract_mainfile_keys' measure_table, 
count(*) measure_value 
from 
( 
    select 
        case 
            when db.operation_code = 'DELT' 
                or eb.operation_code = 'DELT' then 'DELT' 
            else 'NON-DELT' 
        end as operationcode, 
        row_number() over(partition by db.company_code, db.segment_id, db.key_qualifier, db.ordinal_position, eb.fund_number 
                          order by 
                              cast(delta.batchid as int) desc, 
                              cast(db.batchid as int) desc, 
                              cast(eb.batchid as int) desc, 
                              db.segment_sequence_number desc) as seq_num 
    from 
        (select 
            key_qualifier, 
            system_name, 
            batchid, 
            company_code 
        from 
            {source_database}.gdqp5deltaextract_mainfile_keys delta 
        where 
            cast(delta.batchid as int) = (batchid) 
            and delta.system_name = 'PS' 
            and (delta.file_name like 'CLPRODX' 
            and delta.segment_id in ('DB','EB')) 
        group by 
            key_qualifier, 
            system_name, 
            batchid, 
            company_code) delta 
    join 
        (select 
            * 
        from 
            (select 
                key_qualifier, company_code, ordinal_position, segment_id, batchid, uuid, segment_sequence_number, 
                row_number() over(partition by key_qualifier, company_code order by cast(batchid as int) desc, segment_sequence_number desc) as seq_num 
            from 
                {source_database}.gdqp5clprodsegmentdb 
            where 
                cast(batchid as int) <= (batchid)) 
        where seq_num = 1) db 
        on db.key_qualifier = delta.key_qualifier 
        and db.company_code = delta.company_code 
    left join 
        (select 
            * 
        from 
            (select 
                key_qualifier, company_code, operation_code, batchid, 
                row_number() over(partition by key_qualifier, company_code order by cast(batchid as int) desc, 
                (case when operation_code = 'DELT' then 2 else 1 end)) as seq_num 
            from 
                {source_database}.gdqp5deltaextract_mainfile_keys 
            where 
                cast(batchid as int) = (batchid) 
                and file_name like 'CLPRODX' 
                and segment_id = 'DB') 
        where seq_num = 1) delta 
        on db.key_qualifier = delta.key_qualifier 
        and db.company_code = delta.company_code 
    join 
        (select 
            * 
        from 
            (select 
                key_qualifier, company_code, operation_code, batchid, fund_number, 
                row_number() over(partition by key_qualifier, company_code, fund_number 
                                  order by cast(batchid as int) desc, occurs_index desc, segment_sequence_number desc) as seq_num 
            from 
                (select 
                    uuid, company_code, key_qualifier, fund_number, segment_id, segment_sequence_number, occurs_index, operation_code, time_stamp, load_date, batchid 
                from 
                    (select 
                        * 
                    from 
                        (select 
                            segment_id, segment_sequence_number, occurs_index, key_qualifier, company_code, fund_type_indicator_override, uuid, time_stamp, load_date, batchid, fund_number, 
                            row_number() over(partition by key_qualifier, company_code, fund_number order by cast(batchid as int) desc, occurs_index desc) as seq_num 
                        from 
                            {source_database}.gdqp5clprodsegmenteb_fund_information 
                        where 
                            cast(batchid as int) <= (batchid)) 
                    where seq_num = 1) 
                ) 
            where seq_num = 1) eb 
        left join 
            (select 
                * 
            from 
                (select 
                    key_qualifier, company_code, operation_code, batchid, segment_sequence_number, 
                    row_number() over(partition by key_qualifier, company_code, segment_sequence_number 
                                      order by cast(batchid as int) desc, (case when operation_code = 'DELT' then 2 else 1 end)) as seq_num 
                from 
                    {source_database}.gdqp5deltaextract_mainfile_keys 
                where 
                    cast(batchid as int) = (batchid) 
                    and file_name like 'CLPRODX' 
                    and segment_id = 'EB') 
            where seq_num = 1) delta 
            on eb.key_qualifier = delta.key_qualifier 
            and eb.company_code = delta.company_code 
            and eb.segment_sequence_number = delta.segment_sequence_number 
    union all 
    select 
        uuid, company_code, key_qualifier, fund_number, segment_id, segment_sequence_number, occurs_index, operation_code, time_stamp, load_date, batchid 
    from 
        (select 
            * 
        from 
            (select 
                segment_id, segment_sequence_number, occurs_index, key_qualifier, company_code, uuid, time_stamp, load_date, batchid, fund_number, 
                row_number() over(partition by key_qualifier, company_code, fund_number order by cast(batchid as int) desc, occurs_index desc) as seq_num 
            from 
                {source_database}.gdqp5clprodsegmentebu_fund_information 
            where 
                cast(batchid as int) <= (batchid)) 
        where seq_num = 1) ebu 
        left join 
            (select 
                * 
            from 
                (select 
                    key_qualifier, company_code, operation_code, batchid, segment_sequence_number, 
                    row_number() over(partition by key_qualifier, company_code, segment_sequence_number 
                                      order by cast(batchid as int) desc, (case when operation_code = 'DELT' then 2 else 1 end)) as seq_num 
                from 
                    {source_database}.gdqp5deltaextract_mainfile_keys 
                where 
                    cast(batchid as int) = (batchid) 
                    and file_name like 'CLPRODX' 
                    and segment_id = 'EBU') 
            where seq_num = 1) delta 
            on ebu.key_qualifier = delta.key_qualifier 
            and ebu.company_code = delta.company_code 
            and ebu.segment_sequence_number = delta.segment_sequence_number 
) x;


SELECT
    segment_id, segment_sequence_number, occurs_index, key_qualifier, company_code, uuid, time_stamp, load_date, batchid, fund_number
    , row_number() OVER(PARTITION BY key_qualifier,
        company_code,
        fund_number,
        segment_sequence_number
    ORDER BY
        cast(batchid as int) desc,
        occurs_index desc) as seq_num
FROM
    {source_database}.gdqp5clprodsegmentebu_fund_information
WHERE
    cast(batchid as int) <= (batchid)
WHERE
    seq_num = 1) ebu
LEFT JOIN (
SELECT
    *
FROM
    (
    SELECT
        key_qualifier, company_code, operation_code, batchid, segment_sequence_number, row_number() OVER(PARTITION BY key_qualifier,
            company_code,
            segment_sequence_number
        ORDER BY
            cast(batchid as int) desc,
            (case
                when operation_code = 'DELT' then 2
                else 1
            end)) as seq_num
    FROM
        {source_database}.gdqp5deltaextract_mainfile_keys
    WHERE
        cast(batchid as int) <= (batchid)
        and file_name like 'CLPRODX'
        and segment_id = 'EBU')
WHERE
    seq_num = 1) delta ON
    ebu.key_qualifier = delta.key_qualifier
    and ebu.company_code = delta.company_code
    and ebu.segment_sequence_number = delta.segment_sequence_number)) ebebu) a
WHERE
    a.seq_num = 1) eb ON
eb.key_qualifier = delta.key_qualifier
and eb.company_code = delta.company_code)
WHERE
    a.seq_num = 1 and a.operationcode <> 'DELT';
