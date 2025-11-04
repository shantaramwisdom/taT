select *
from time_sch.rr_ra09_policy_loan_mppng
where 'Y' = '{ifrs17_pattern}'
    and contract_source_system in ({ifrs_originating_systems});
