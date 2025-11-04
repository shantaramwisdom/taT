select 
eb.*,
operation_code
from 
(
    select 
    *,
    row_number() over(partition by key_qualifier,
                                    company_code,
                                    fund_number
                      order by 
                          cast(batchid as int) desc,
                          occurs_index desc,
                          segment_sequence_number desc) as seq_num
    from 
    (
        select 
        segment_sequence_number,
        occurs_index,
        coalesce(fund_number_override, c_fund_number, fund_number) as contractoptionfundnumber,
        key_qualifier,
        company_code,
        coalesce(fund_type_indicator_override, c_fund_type_indicator, fund_type_indicator) as contractoptionfundclassindicator,
        batchid,
        fund_number,
        row_number() over(partition by key_qualifier,
                                        company_code,
                                        fund_number,
                                        segment_sequence_number
                          order by 
                              cast(batchid as int) desc,
                              occurs_index desc) as row_num
        from 
        {source_database}.gdqp5dprodsegmentdb_fund_information
        where 
        cast(batchid as int) <= {batchid}
    ) eb
    where 
    row_num = 1
) eb
left join gdqp5deltaextract_mainfile_keys delta on 
eb.key_qualifier = delta.key_qualifier
and eb.company_code = delta.company_code
and eb.segment_sequence_number = delta.segment_sequence_number
and delta.segment_id = 'EB'
where 
eb.seq_num = 1
