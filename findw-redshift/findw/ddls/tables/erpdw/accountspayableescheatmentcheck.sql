CREATE TABLE IF NOT EXISTS erpdw.accounts_payablesescheatmentcheck
(
 batchid INTEGER NOT NULL
 ,cycledate DATE NOT NULL
 ,source_system_name VARCHAR(50) NOT NULL
 ,naturalkeyhashvalue VARCHAR(255) NOT NULL
 ,hashvalue VARCHAR(255) NOT NULL
 ,inserttimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,updatetimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,pointofviewstartdate TIMESTAMP WITHOUT TIME ZONE NOT NULL
 ,pointofviewstopdate TIMESTAMP WITHOUT TIME ZONE NOT NULL
 ,accountspayablesescheatmentcheckid VARCHAR(255) NOT NULL
 ,checkidentifier VARCHAR(30)
 ,externaltransactionidentifier VARCHAR(30)
 ,sourcesystemname VARCHAR(50) NOT NULL
 ,documentid VARCHAR(255) NOT NULL
 ,checkamount Decimal(18,2)
 ,checknumber VARCHAR(25)
 ,escheatmentcheckstatus VARCHAR(50)
 ,extendedsuppliersresidentcountrycode VARCHAR(5)
 ,invoicepaygroup VARCHAR(25)
 ,invoicepaymentdate Date
 ,remittancemessage VARCHAR(150)
 ,replacementcheckidentifier VARCHAR(30)
 ,sourcesystemsupplieridentifier VARCHAR(30)
 ,sourcesystemsuppliername VARCHAR(240)
 ,supplieraddressline1 VARCHAR(240)
 ,supplieraddressline2 VARCHAR(240)
 ,supplieraddressline3 VARCHAR(240)
 ,supplieraddressline4 VARCHAR(240)
 ,suppliercityname VARCHAR(60)
 ,supplierpostalcode VARCHAR(15)
 ,supplierstatecode VARCHAR(5)
 ,taxidentificationnumber VARCHAR(255)
 ,PRIMARY KEY (accountspayablesescheatmentcheckid)
 ,UNIQUE (checkidentifier, externaltransactionidentifier)
 ,UNIQUE (pointofviewstartdate, source_system_name, naturalkeyhashvalue)
 ,UNIQUE (pointofviewstartdate, source_system_name, documentid)
)
DISTSTYLE KEY
DISTKEY (checkidentifier)
SORTKEY (
 cycledate, source_system_name
);
