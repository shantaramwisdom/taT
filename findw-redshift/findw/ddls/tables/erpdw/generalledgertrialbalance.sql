CREATE TABLE IF NOT EXISTS erpdw.generalledgertrialbalance
(
 batchid BIGINT NOT NULL
 ,cycledate DATE NOT NULL
 ,source_system_name VARCHAR(50) NOT NULL
 ,naturalkeyhashvalue VARCHAR(255) NOT NULL
 ,hashvalue VARCHAR(255) NOT NULL
 ,inserttimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,updatetimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,pointofviewstartdate TIMESTAMP WITHOUT TIME ZONE NOT NULL
 ,pointofviewstopdate TIMESTAMP WITHOUT TIME ZONE NOT NULL
 ,documentid VARCHAR(255) NOT NULL
 ,sourcesystemname VARCHAR(50) NOT NULL
 ,generalledgeraccountnumber VARCHAR(60)
 ,generalledgerledgername VARCHAR(30)
 ,chartofaccountscompanycode VARCHAR(5)
 ,chartofaccountslineofbusinesscode VARCHAR(4)
 ,chartofaccountsdepartmentcode VARCHAR(5)
 ,chartofaccountsaccountcode VARCHAR(6)
 ,chartofaccountsproductcode VARCHAR(10)
 ,chartofaccountsintercompanycode VARCHAR(5)
 ,chartofaccountssummaryproductcode VARCHAR(5)
 ,generalledgeraccountingyear INTEGER
 ,generalledgeraccountingmonth INTEGER
 ,geacledgercode VARCHAR(10)
 ,generalledgerdescription VARCHAR(255)
 ,legalcompanycode VARCHAR(255)
 ,legalcompanyname VARCHAR(255)
 ,generalledgerreportingcurrencycode VARCHAR(60)
 ,trialbalancetransactionbeginningbalance NUMERIC(18,6)
 ,trialbalancetransactiontotaldebits NUMERIC(18,6)
 ,trialbalancetransactiontotalcredits NUMERIC(18,6)
 ,trialbalancetransactionendingbalance NUMERIC(18,6)
 ,trialbalancereportingtotaldebits NUMERIC(18,6)
 ,trialbalancereportingtotalcredits NUMERIC(18,6)
 ,trialbalancereportingbeginningbalance NUMERIC(18,6)
 ,trialbalancereportingendingbalance NUMERIC(18,6)
 ,generalledgeraccountnumber VARCHAR(255)
 ,generalledgeraccountsuffix VARCHAR(255)
 ,centernumber VARCHAR(255)
 ,generalledgerlineofbusinessdescription VARCHAR(255)
)
DISTSTYLE AUTO
SORTKEY (
 cycledate, source_system_name
);
