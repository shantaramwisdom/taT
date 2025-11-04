CREATE TABLE IF NOT EXISTS erpdw.generalledgerjournaldetail
(
 batchid INT NOT NULL
 ,cycledate DATE NOT NULL
 ,source_system_name VARCHAR(50) NOT NULL
 ,inserttimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,updatetimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,generalledgerjournalbatchname VARCHAR(240) NOT NULL
 ,generalledgerjournalname VARCHAR(400) NOT NULL
 ,generalledgerjournallinenumber INTEGER NOT NULL
 ,generalledgerjournallinecode VARCHAR(10)
 ,glacedgercode VARCHAR(10)
 ,generalledgerjournalnamedescription VARCHAR(960) NOT NULL
 ,generalledgerjournallinedescription VARCHAR(4000)
 ,generalledgertransactioncurrencycode VARCHAR(60)
 ,generalledgertransactioncurrencyamount DECIMAL(18,6) NOT NULL
 ,generalledgerreportingcurrencycode VARCHAR(60)
 ,generalledgerreportingcurrencyamount DECIMAL(18,6)
 ,generalledgerstatisticalamount DECIMAL(18,6)
 ,generalledgercurrencyconversiondate DATE
 ,generalledgercurrencyconversionratetype VARCHAR(120)
 ,generalledgerreportingcurrencyexchangerate DECIMAL(18,6)
 ,generalledgerjournalsource VARCHAR(240)
 ,generalledgerjournalsourcecategory VARCHAR(100)
 ,generalledgeraccountingperiod VARCHAR(60)
 ,generalledgeraccountingyear INTEGER
 ,generalledgeraccountingmonth INTEGER
 ,activityreporteddate TIMESTAMP
 ,generalledgerjournalstatus VARCHAR(4)
 ,generalledgerledgername VARCHAR(240)
 ,chartofaccountscompanycode VARCHAR(3)
 ,chartofaccountslineofbusinesscode VARCHAR(4)
 ,chartofaccountsdepartmentcode VARCHAR(6)
 ,chartofaccountsaccountcode VARCHAR(6)
 ,chartofaccountsproductcode VARCHAR(6)
 ,chartofaccountsintercompanycode VARCHAR(5)
 ,chartofaccountssummaryproductcode VARCHAR(3)
 ,generalledgerjournalcreator VARCHAR(256)
 ,generalledgerjournalreversalindicator VARCHAR(255)
 ,generalledgeroriginaljournalname VARCHAR(400)
 ,generalledgerreversaldeate TIMESTAMP
 ,generalledgerjournalcreatedate TIMESTAMP
 ,generalledgerjournalpostdate TIMESTAMP
 ,generalledgerapprovalstatus VARCHAR(30)
 ,legalcompanycode VARCHAR(255)
 ,generalledgeraccountnumber VARCHAR(255)
 ,generalledgeraccountsuffix VARCHAR(255)
 ,centernumber VARCHAR(255)
 ,PRIMARY KEY (source_system_name, generalledgerjournalbatchname, generalledgerjournalname, generalledgerjournallinenumber)
 ,UNIQUE (cycledate, source_system_name, generalledgerjournalbatchname, generalledgerjournalname, generalledgerjournallinenumber)
)
DISTSTYLE AUTO
SORTKEY (
 cycledate, source_system_name, generalledgeraccountingperiod, chartofaccountscompanycode,
 chartofaccountslineofbusinesscode, chartofaccountsdepartmentcode, chartofaccountsaccountcode,
 chartofaccountsproductcode, chartofaccountsintercompanycode
);
