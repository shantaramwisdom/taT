CREATE TABLE IF NOT EXISTS erpdw.oracleworkercountry
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
 ,employeecountryname VARCHAR(450) NOT NULL
 ,sourcesystemname VARCHAR(50) NOT NULL
 ,documentid VARCHAR(255) NOT NULL
 ,includeexclude VARCHAR(25)
 ,PRIMARY KEY (employeecountryname)
 ,UNIQUE (pointofviewstartdate, source_system_name, documentid)
 ,UNIQUE (pointofviewstartdate, source_system_name, naturalkeyhashvalue)
)
DISTSTYLE KEY
DISTKEY (employeecountryname)
SORTKEY (
 cycledate, source_system_name
);
