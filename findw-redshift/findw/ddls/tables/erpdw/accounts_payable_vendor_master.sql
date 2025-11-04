CREATE TABLE IF NOT EXISTS erpdw.accounts_payable_vendor_master
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
 ,sourcesystemsupplieridentifier VARCHAR(30) NOT NULL
 ,suppliersiteidentifier VARCHAR(15) NOT NULL
 ,sourcesystemname VARCHAR(50) NOT NULL
 ,documentid VARCHAR(255) NOT NULL
 ,businessunit VARCHAR(240)
 ,payeebankaccountnumber VARCHAR(255)
 ,incometaxpocode VARCHAR(10)
 ,invoicepaygroup VARCHAR(25)
 ,invoicepaymentmethodcode VARCHAR(30)
 ,invoicepaymentterms VARCHAR(50)
 ,payeeautomatedclearinghousestandardentryclasscode VARCHAR(10)
 ,payeebankaccounttype VARCHAR(25)
 ,payeebankroutingtransitnumber VARCHAR(255)
 ,sourcesystemsuppliername VARCHAR(240)
 ,sourcesystemvendorgroup VARCHAR(30)
 ,supplieraddressline1 VARCHAR(240)
 ,supplieraddressline2 VARCHAR(240)
 ,supplieraddressline3 VARCHAR(240)
 ,supplieraddressline4 VARCHAR(240)
 ,suppliercityname VARCHAR(60)
 ,supplierpostalcode VARCHAR(15)
 ,suppliersresidentcountrycode VARCHAR(5)
 ,supplierstatecode VARCHAR(5)
 ,suppliertype VARCHAR(30)
 ,taxidentificationnumber VARCHAR(255)
 ,taxorganizationtype VARCHAR(15)
 ,taxreportingname VARCHAR(255)
 ,invoicepayaloneidentifier VARCHAR(1)
 ,sourcesystemincometaxpocode VARCHAR(10)
 ,sourcesysteminvoicepaymentterms VARCHAR(50)
 ,remittancemailidentifier VARCHAR(255)
 ,sourcesystempayeebankaccounttype VARCHAR(25)
 ,sourcesystempayeebankaccountcode VARCHAR(15)
 ,PRIMARY KEY (sourcesystemsupplieridentifier, suppliersiteidentifier)
 ,UNIQUE (cycledate, batchid, source_system_name, documentid)
 ,UNIQUE (pointofviewstartdate, source_system_name, naturalkeyhashvalue)
 ,FOREIGN KEY (sourcesystemsupplieridentifier, suppliersiteidentifier) REFERENCES erpdw.accounts_payable_vendor(sourcesystemsupplieridentifier, suppliersiteidentifier)
)
DISTSTYLE KEY
DISTKEY (documentid)
SORTKEY (
 cycledate, source_system_name
);
