CREATE TABLE IF NOT EXISTS erpdw.oracleworkerdetails
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
 ,sailpointemployeeid VARCHAR(25) NOT NULL
 ,sourcesystemname VARCHAR(50) NOT NULL
 ,employeecountryname VARCHAR(450)
 ,documentid VARCHAR(255) NOT NULL
 ,employeelastname VARCHAR(255)
 ,employeefirstname VARCHAR(255)
 ,employeemiddlename VARCHAR(255)
 ,employeeemailaddress VARCHAR(255)
 ,employeetype VARCHAR(450)
 ,employeesupervisorname1 VARCHAR(450)
 ,supervisorsailpointemployeeid VARCHAR(50)
 ,PRIMARY KEY (sailpointemployeeid)
 ,UNIQUE (pointofviewstartdate, source_system_name, documentid)
 ,UNIQUE (pointofviewstartdate, source_system_name, naturalkeyhashvalue)
 ,FOREIGN KEY (employeecountryname) REFERENCES erpdw.oracleworkercountry(employeecountryname)
)
DISTSTYLE KEY
DISTKEY (sailpointemployeeid)
SORTKEY (
 cycledate, source_system_name
);
