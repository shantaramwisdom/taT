CREATE TABLE IF NOT EXISTS erpdw.paymentdetail
(
 batchid INTEGER NOT NULL
 ,cycledate DATE NOT NULL
 ,source_system_name VARCHAR(50) NOT NULL
 ,inserttimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,updatetimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,sourcesystemsupplieridentifier VarChar(30) NOT NULL
 ,invoicenumber VarChar(50) NOT NULL
 ,invoiceissuedate Date NOT NULL
 ,invoicesequencenumber INTEGER NOT NULL
 ,invoicelinenumber Integer NOT NULL
 ,businessunit VarChar(240)
 ,sourcesystemsuppliername VarChar(240)
 ,invoicepaymentstatus VarChar(10)
 ,remittancemessage VarChar(150)
 ,invoicepaymentdate Date
 ,invoicepaymentamount Decimal(18,2)
 ,checknumber VarChar(25)
 ,invoicepaymenttype VarChar(1)
 ,priorinvoicepaymentdate Date
 ,priorchecknumber VarChar(10)
 ,priorinvoicepaymenttype VarChar(1)
 ,invoicetryreporteddate Timestamp
 ,originatinggeneralledgerdescription1 VarChar(30)
 ,activityamount Decimal(18,2)
 ,originatinggeneralledgerdescription2 VarChar(30)
 ,chartofaccountscompanycode VarChar(5)
 ,chartofaccountslineofbusinesscode VarChar(4)
 ,chartofaccountsdepartmentcode VarChar(5)
 ,chartofaccountsaccountcode VarChar(6)
 ,chartofaccountsproductcode VarChar(10)
 ,chartofaccountsintercompanycode VarChar(5)
 ,chartofaccountssummaryproductcode VarChar(5)
 ,legalcompanycode VarChar(255)
 ,legalglcode VarChar(10)
 ,generalledgeraccountnumber VarChar(255)
 ,generalledgeraccountsuffix VarChar(255)
 ,centernumber VarChar(255)
 ,invoicelinedistributionpackeddebitamount VarChar(15)
 ,UNIQUE (cycledate, batchid, source_system_name, sourcesystemsupplieridentifier, invoicenumber, invoiceissuedate, invoicesequencenumber, invoicelinenumber)
)
DISTSTYLE AUTO
SORTKEY (
 cycledate, source_system_name
);
