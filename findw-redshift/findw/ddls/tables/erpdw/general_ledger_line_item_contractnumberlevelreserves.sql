CREATE TABLE IF NOT EXISTS erpdw.general_ledger_line_item_contractnumberlevelreserves
(
 batchid INTEGER NOT NULL
 ,cycledate DATE NOT NULL
 ,source_system_name VARCHAR(50) NOT NULL
 ,inserttimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,updatetimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,original_cycle_date DATE
 ,original_batch_id INTEGER
 ,activity_accounting_id CHAR(30) PRIMARY KEY NOT NULL
 ,transaction_number VARCHAR(50) NOT NULL
 ,line_number INTEGER NOT NULL
 ,default_amount decimal(18,2) NOT NULL
 ,debit_credit_indicator VARCHAR(100)
 ,orig_gl_company VARCHAR(30)
 ,orig_gl_account VARCHAR(30)
 ,orig_gl_center VARCHAR(30)
 ,groupcontractnumber VARCHAR(15)
 ,contractnumber VARCHAR(15)
 ,contractsourcesystemname VARCHAR(30)
 ,ifrs17cohort VARCHAR(100)
 ,ifrs17grouping VARCHAR(100)
 ,ifrs17measurementmodel VARCHAR(30)
 ,ifrs17portfolio VARCHAR(100)
 ,ifrs17profitability VARCHAR(100)
 ,ifrs17reportingcashflowtype VARCHAR(30)
 ,statutoryresidentcountrycode VARCHAR(5)
 ,statutoryresidentstatecode VARCHAR(5)
 ,activityreversalcode VARCHAR(1)
 ,contractissuedate Date
 ,contractissuestatecode VARCHAR(5)
 ,plancode VARCHAR(255)
 ,reinsuranceassumedcededflag VARCHAR(30)
 ,reinsurancecounterpartytype VARCHAR(30)
 ,reinsurancetreatybasis VARCHAR(30)
 ,reinsurancetreatynumber VARCHAR(30)
 ,cessionidentifier VARCHAR(30)
 ,contractparticipationindicator VARCHAR(30)
 ,contractdurationinyears INTEGER
 ,reinsuranceaccountflag VARCHAR(30)
 ,reinsurancegroupindividualflag VARCHAR(30)
 ,reinsurancetreatycomponentidentifier VARCHAR(30)
 ,sourcelegalentitycode VARCHAR(10)
 ,sourcesystemreinsurancecounterpartycode VARCHAR(30)
 ,typeofbusinessreinsured VARCHAR(30)
 ,UNIQUE (cycledate, batchid, source_system_name, transaction_number, line_number)
 ,FOREIGN KEY (transaction_number) REFERENCES erpdw.general_ledger_header_contractnumberlevelreserves(transaction_number)
)
DISTSTYLE KEY
DISTKEY (transaction_number)
SORTKEY (
 cycledate, source_system_name
);
