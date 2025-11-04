CREATE TABLE IF NOT EXISTS erpdw.accounts_payablesescheatmentinvoice
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
 ,accountspayablesescheatmentinvoiceid VARCHAR(255) NOT NULL
 ,checkidentifier VARCHAR(30)
 ,externaltransactionidentifier VARCHAR(30)
 ,invoiceidentifier VARCHAR(30)
 ,accountspayablesescheatmentcheckid VARCHAR(255)
 ,sourcesystemname VARCHAR(50) NOT NULL
 ,documentid VARCHAR(255) NOT NULL
 ,admisystembupflag VARCHAR(1)
 ,businessunit VARCHAR(240)
 ,chartofaccountsaccountcode VARCHAR(6)
 ,chartofaccountscompanycode VARCHAR(5)
 ,chartofaccountsdepartmentcode VARCHAR(5)
 ,chartofaccountscompanycode VARCHAR(5)
 ,chartofaccountslineofbusinesscode VARCHAR(5)
 ,chartofaccountsproductcode VARCHAR(5)
 ,chartofaccountsprogramcode VARCHAR(5)
 ,escheatmentpropertytype VARCHAR(34)
 ,generalledgerapplicationareacode VARCHAR(30)
 ,generalledgername VARCHAR(120)
 ,invoiceamount DECIMAL(18,2)
 ,invoicesubjectof DATE
 ,invoicenumber VARCHAR(50)
 ,invoicetype VARCHAR(25)
 ,originatinggeneralledgerdescription1 VARCHAR(30)
 ,originatinggeneralledgerdescription2 VARCHAR(30)
 ,originatinggeneralledgerdescription3 VARCHAR(30)
 ,originatinggeneralledgerprojectcode VARCHAR(30)
 ,replacementcheckidentifier VARCHAR(30)
 ,invoicepaygroup VARCHAR(25)
 ,sourcesystemsupplieridentifier VARCHAR(30)
 ,PRIMARY KEY (accountspayablesescheatmentinvoiceid)
 ,UNIQUE (checkidentifier, externaltransactionidentifier, invoiceidentifier)
 ,UNIQUE (pointofviewstartdate, source_system_name, naturalkeyhashvalue)
 ,UNIQUE (pointofviewstartdate, source_system_name, documentid)
 ,FOREIGN KEY (accountspayablesescheatmentcheckid) REFERENCES erpdw.accounts_payablesescheatmentcheck(accountspayablesescheatmentcheckid)
)
DISTSTYLE KEY
DISTKEY (documentid)
SORTKEY (
 cycledate, source_system_name
);
