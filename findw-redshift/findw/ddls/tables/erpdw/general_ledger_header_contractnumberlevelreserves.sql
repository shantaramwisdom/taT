CREATE TABLE IF NOT EXISTS erpdw.general_ledger_header_contractnumberlevelreserves
(
 batchid INTEGER NOT NULL
 ,cycledate DATE NOT NULL
 ,source_system_name VARCHAR(50) NOT NULL
 ,inserttimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,updatetimestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'US/Central')
 ,original_cycle_date DATE
 ,original_batch_id INTEGER
 ,transaction_number VARCHAR(50) NOT NULL
 ,source_system_nm VARCHAR(30) NOT NULL
 ,ledger_name VARCHAR(100) NOT NULL
 ,subledger_short_name VARCHAR(100) NOT NULL
 ,event_type_code VARCHAR(100) NOT NULL
 ,transaction_date DATE NOT NULL
 ,contractnumber VARCHAR(15)
 ,contractsourcesystemname VARCHAR(30)
 ,gl_reversal_date DATE
 ,secondary_ledger_code VARCHAR(100)
 ,data_type VARCHAR(30)
 ,PRIMARY KEY (transaction_number)
)
DISTSTYLE KEY
DISTKEY (transaction_number)
SORTKEY (
 cycledate, source_system_name
);
