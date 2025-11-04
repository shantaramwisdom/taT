CREATE TABLE IF NOT EXISTS erpdw.currency_rates
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
 ,generalledgertransactioncurrencycode VARCHAR(5) NOT NULL
 ,generalledgerreportingcurrencycode VARCHAR(5) NOT NULL
 ,generalledgerreportingcurrencyconversionratetype VARCHAR(50) NOT NULL
 ,generalledgerreportingcurrencyexchangeeffectiveperiodstartdate DATE NOT NULL
 ,generalledgerreportingcurrencyexchangeeffectiveperiodstopdate DATE
 ,generalledgerreportingcurrencyexchangerate DECIMAL(18,6)
 ,PRIMARY KEY (generalledgertransactioncurrencycode, generalledgerreportingcurrencycode, generalledgerreportingcurrencyexchangeeffectiveperiodstartdate, generalledgerreportingcurrencyconversionratetype)
);
