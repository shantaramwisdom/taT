CREATE TABLE IF NOT EXISTS erpdw.accounts_payable_header
(
 batchid INTEGER NOT NULL
 ,cycledate DATE NOT NULL
 ,source_system_name VARCHAR(50) NOT NULL
 ,inserttimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,updatetimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,original_cycle_date DATE
 ,original_batch_id INTEGER
 ,invoiceidentifier VARCHAR(50) PRIMARY KEY NOT NULL
 ,sourcesystempaygroup VARCHAR(25) NOT NULL
 ,sourcesystembatchdate DATE NOT NULL
 ,sourcesystembatchidentifier VARCHAR(255)
 ,generalledgerapplicationareacode VARCHAR(30)
 ,sourcesystemsupplieridentifier VARCHAR(30) NOT NULL
 ,supplieridentifier VARCHAR(30)
 ,invoicenumber VARCHAR(25) NOT NULL
 ,invoiceissuedate DATE NOT NULL
 ,activityreporteddate DATE NOT NULL
 ,businessunit VARCHAR(240) NOT NULL
 ,checknumber VARCHAR(25)
 ,escheatmentpropertytype VARCHAR(30)
 ,generalledgersourcecode VARCHAR(30)
 ,invoiceamount DECIMAL(18,2)
 ,invoicepaygroup VARCHAR(25) NOT NULL
 ,invoicepaymentdate DATE
 ,invoicepaymentspecialhandlingcode VARCHAR(30)
 ,invoicestatus VARCHAR(30)
 ,invoicetype VARCHAR(25)
 ,legalcompanyname VARCHAR(60) NOT NULL
 ,remittancemessage VARCHAR(150)
 ,sourcesystemname VARCHAR(50)
 ,sourcesystempayorbankaccountcode VARCHAR(15)
 ,sourcesystemvendorgroup VARCHAR(30)
 ,suppliersiteidentifier VARCHAR(15)
 ,invoicepaymentmethodcode VARCHAR(30)
 ,payorbankaccountnumber VARCHAR(255)
 ,sourcesystemaskedpayorbankaccountnumber VARCHAR(30)
 ,preoracleflag VARCHAR(1)
 ,src_nmbr VARCHAR(20)
 ,seq_nmbr VARCHAR(20)
 ,record_type VARCHAR(15)
 ,suppliername VARCHAR(240)
)
DISTSTYLE KEY
DISTKEY (invoiceidentifier)
SORTKEY (
 cycledate, source_system_name
);
