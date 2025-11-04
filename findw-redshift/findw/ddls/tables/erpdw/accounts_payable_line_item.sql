CREATE TABLE IF NOT EXISTS erpdw.accounts_payable_line_item
(
 batchid INTEGER NOT NULL
 ,cycledate DATE NOT NULL
 ,source_system_name VARCHAR(50) NOT NULL
 ,inserttimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,updatetimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,original_cycle_date DATE
 ,original_batch_id INTEGER
 ,activity_accounting_id VARCHAR(50) PRIMARY KEY NOT NULL
 ,invoiceidentifier VARCHAR(50) NOT NULL
 ,invoicelinenumber INTEGER NOT NULL
 ,activityamount DECIMAL(18,2)
 ,originatinggeneralledgeraccountnumber VARCHAR(30) NOT NULL
 ,originatinggeneralledgercenternumber VARCHAR(30) NOT NULL
 ,originatinggeneralledgercompanycode VARCHAR(30) NOT NULL
 ,originatinggeneralledgerdescription1 VARCHAR(30)
 ,originatinggeneralledgerdescription2 VARCHAR(30)
 ,originatinggeneralledgerdescription3 VARCHAR(30)
 ,originatinggeneralledgerexpenseindicator VARCHAR(30)
 ,originatinggeneralledgerprojectcode VARCHAR(30)
 ,legalcompanyname VARCHAR(60) NOT NULL
 ,bapcode VARCHAR(10)
 ,partnumber VARCHAR(30)
 ,UNIQUE (cycledate, batchid, source_system_name, invoiceidentifier, invoicelinenumber)
 ,FOREIGN KEY (invoiceidentifier) REFERENCES erpdw.accounts_payable_header(invoiceidentifier)
)
DISTSTYLE KEY
DISTKEY (invoiceidentifier)
SORTKEY (
 cycledate, source_system_name
);
