CREATE TABLE IF NOT EXISTS erpdw.contractplancodedetails
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
 ,documentid VARCHAR(255) NOT NULL
 ,sourcesystemname VARCHAR(50) NOT NULL
 ,contractsourcesystemname VARCHAR(50) NOT NULL
 ,contractnumber VARCHAR(15) NOT NULL
 ,plancode VARCHAR(255)
 ,contractissuaged INTEGER
 ,contractparticipatingindicator VARCHAR(50)
 ,contractsortmarketingorganizationcode VARCHAR(50)
 ,firstyearindicator VARCHAR(20)
 ,blockofbusiness VARCHAR(50)
 ,modelname VARCHAR(50)
 ,PRIMARY KEY (contractnumber, contractsourcesystemname)
 ,UNIQUE (cycledate, batchid, source_system_name, contractnumber, contractsourcesystemname)
)
DISTSTYLE AUTO
SORTKEY (
 pointofviewstopdate, source_system_name
);
