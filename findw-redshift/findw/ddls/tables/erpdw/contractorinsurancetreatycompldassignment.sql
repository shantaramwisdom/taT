CREATE TABLE IF NOT EXISTS erpdw.contracttoreinsurancetreatycompidassignment
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
 ,reinsurancetreatycomponentidentifier VARCHAR(255) NOT NULL
 ,reinsurancetreatynumber VARCHAR(255) NOT NULL
 ,PRIMARY KEY (contractsourcesystemname, contractnumber, reinsurancetreatycomponentidentifier, reinsurancetreatynumber)
 ,UNIQUE (cycledate, batchid, source_system_name, contractnumber, contractsourcesystemname, reinsurancetreatycomponentidentifier, reinsurancetreatynumber)
)
DISTSTYLE AUTO
SORTKEY (
 pointofviewstopdate, source_system_name
);
