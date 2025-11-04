CREATE TABLE IF NOT EXISTS erpdw.edmcshierarchy
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
 ,hierarchyname VARCHAR(50) NOT NULL
 ,name VARCHAR(15)
 ,description VARCHAR(255)
 ,parent VARCHAR(15)
 ,enabled VARCHAR(1)
 ,summary VARCHAR(1)
 ,allowposting VARCHAR(1)
 ,allowbudgeting VARCHAR(1)
 ,accounttype VARCHAR(15)
 ,thirdpartyaccountcontrol VARCHAR(1)
 ,financialcategory VARCHAR(20)
 ,reconcile VARCHAR(1)
 ,level1 VARCHAR(10)
 ,level2 VARCHAR(255)
 ,level3 VARCHAR(255)
 ,level4 VARCHAR(255)
 ,level5 VARCHAR(255)
 ,level6 VARCHAR(255)
 ,level7 VARCHAR(255)
 ,level8 VARCHAR(255)
 ,level9 VARCHAR(255)
 ,level10 VARCHAR(255)
 ,level11 VARCHAR(255)
 ,level12 VARCHAR(255)
 ,level13 VARCHAR(255)
 ,level14 VARCHAR(255)
 ,level15 VARCHAR(255)
 ,level16 VARCHAR(255)
 ,level17 VARCHAR(255)
 ,level18 VARCHAR(255)
 ,level19 VARCHAR(255)
 ,level20 VARCHAR(255)
 ,PRIMARY KEY (hierarchyname, name)
 ,UNIQUE (pointofviewstartdate, source_system_name, documentid)
 ,UNIQUE (pointofviewstartdate, source_system_name, naturalkeyhashvalue)
)
DISTSTYLE KEY
DISTKEY (hierarchyname)
SORTKEY (
 cycledate, source_system_name
);
