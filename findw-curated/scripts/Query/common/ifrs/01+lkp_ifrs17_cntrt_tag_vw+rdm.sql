with
data as (
    select a.*
    from (
        select a.*,
            row_number() over (
                partition by origntng_cntrct_nbr
                order by null
            ) as fltr
        from time_sch.lkp_ifrs17_cntrt_tag_vw a
        where ORIGNTNG_SRC_SYS_NM in ({ifrs_originating_systems})
    ) a
),
dups as (
    select origntng_cntrct_nbr
    from data
    where fltr > 1
    group by origntng_cntrct_nbr
)
select a.*
from data a
where not exists (
    select 1
    from dups d
    where d.origntng_cntrct_nbr = a.origntng_cntrct_nbr
);
