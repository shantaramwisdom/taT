# transamerica.product.life.group.findw-redshift
This contains sql scripts to be executed on redshift
## Folder structure
configs/deploy .config - contains list of .sql file paths to be executed during deploy
configs/rollback .config - contains list of .sql file paths to be executed during rollback
findw/*/*.sql - sql files contains ddl, dml scripts
redshift-sql-runner.py - Used by Jenkins job to execute sql scripts
redshift-sql-validator.py - Used by Jenkins job to validate configuration and inputs used to run sql scripts
## Deploy target
Deployed to aws redshift:
DEV -
1. aws account: agtawsus-ta-data-lake-nonprod (209497537643)
2. cluster name: ta-individual-findw-dev
QA -
1. aws account: agtawsus-ta-data-lake-nonprod (209497537643)
2. cluster name: ta-individual-findw-tst
model -
1. aws account: agtawsus-ta-data-lake-model (510200178882)
2. cluster name: ta-individual-findw-mdl
prod -
1. aws account: agtawsus-ta-data-lake-prod (492931852779)
2. cluster name: ta-individual-findw-prd
