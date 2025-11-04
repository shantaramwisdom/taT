SELECT originatingcontractnumber as ORIGNTNG_CNTRCT_NBR,
       originatingsourcesystemname as ORIGNTNG_SRC_SYS_NM,
       contractifrs17cohort as IFRS17_COHORT,
       contractifrs17grouping as IFRS17_GRPNG,
       contractifrs17portfolio as IFRS17_PRTFOLIO,
       contractifrs17profitability as IFRS17_PRFTBLY
from ifrscontracttagging
where rdm_insert_flag = 'Y'
