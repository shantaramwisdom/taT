SELECT
contractnumber as ORIGINTG_CNTRCT_NBR,
sourcesystemname as ORIGINTG_SRC_SYS_NM,
contractifrs17cohort as IFRS17_COHORT,
contractifrs17grouping as IFRS17_GRPNG,
contractifrs17portfolio as IFRS17_PRTFOLIO,
contractifrs17profitability as IFRS17_PRFTBLTY
from ifrscontracttagging
where rdm_insert_flag = 'Y'
