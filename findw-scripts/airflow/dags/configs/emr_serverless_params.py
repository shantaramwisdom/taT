params = {
    "emr_sizes": {
        "large": {
            "initialCapacity": {
                "DRIVER": {
                    "workerConfiguration": {
                        "cpu": "16vCPU",
                        "disk": "200GB",
                        "memory": "120GB"
                    },
                    "workerCount": 1
                },
                "EXECUTOR": {
                    "workerConfiguration": {
                        "cpu": "16vCPU",
                        "disk": "200GB",
                        "memory": "120GB"
                    },
                    "workerCount": 1
                }
            },
            "maximumCapacity": {
                "cpu": "1600vCPU",
                "disk": "20000GB",
                "memory": "12000GB"
            },
            "sparkSubmitParameters": "--conf spark.executor.instances=25"
        },
        "medium": {
            "initialCapacity": {
                "DRIVER": {
                    "workerConfiguration": {
                        "cpu": "8vCPU",
                        "disk": "100GB",
                        "memory": "60GB"
                    },
                    "workerCount": 1
                },
                "EXECUTOR": {
                    "workerConfiguration": {
                        "cpu": "8vCPU",
                        "disk": "100GB",
                        "memory": "60GB"
                    },
                    "workerCount": 1
                }
            },
            "maximumCapacity": {
                "cpu": "800vCPU",
                "disk": "10000GB",
                "memory": "6000GB"
            },
            "sparkSubmitParameters": "--conf spark.executor.instances=20"
        },
        "small": {
            "initialCapacity": {
                "DRIVER": {
                    "workerConfiguration": {
                        "cpu": "4vCPU",
                        "disk": "50GB",
                        "memory": "30GB"
                    },
                    "workerCount": 1
                },
                "EXECUTOR": {
                    "workerConfiguration": {
                        "cpu": "4vCPU",
                        "disk": "50GB",
                        "memory": "30GB"
                    },
                    "workerCount": 1
                }
            },
            "maximumCapacity": {"cpu": "400vCPU", "disk": "5000GB", "memory": "3000GB"},
            "sparkSubmitParameters": "--conf spark.executor.instances=10"
        },
        "tiny": {
            "initialCapacity": {
                "DRIVER": {
                    "workerConfiguration": {
                        "cpu": "2vCPU",
                        "disk": "25GB",
                        "memory": "15GB"
                    },
                    "workerCount": 1
                },
                "EXECUTOR": {
                    "workerConfiguration": {
                        "cpu": "2vCPU",
                        "disk": "25GB",
                        "memory": "15GB"
                    },
                    "workerCount": 1
                }
            },
            "maximumCapacity": {"cpu": "200vCPU", "disk": "2500GB", "memory": "1500GB"},
            "sparkSubmitParameters": "--conf spark.executor.instances=10"
        }
    },
    "emr_network_details": {
        "dev": {
            "securityGroupIds": [
                "sg-0605491d847dae989",
                "sg-0de9d2e400381872b",
                "sg-0276dc6b9ea53ce67",
                "sg-02405d87f5a34c167",
                "sg-04889874abb301580"
            ],
            "subnetIds": [
                "subnet-0f4481e7a214e774a",
                "subnet-03e4e31fa2c8e1618",
                "subnet-0662ffb480a7b1021"
            ]
        },
        "mdl": {
            "securityGroupIds": [
                "sg-0804910920745a511",
                "sg-02532164b24ae1444",
                "sg-015397419d9821636",
                "sg-070cc0e4895691579",
                "sg-0840449d1d24e51ce"
            ],
            "subnetIds": [
                "subnet-038947a53634af253",
                "subnet-061a123fd16261202",
                "subnet-04ef5a6123539be50"
            ]
        },
        "prd": {
            "securityGroupIds": [
                "sg-00e4daa358a37539a",
                "sg-092e89129d88bcbbe",
                "sg-0f19219bf1edd347b",
                "sg-01baf703b6981053",
                "sg-004d74a0983fa13a3"
            ],
            "subnetIds": [
                "subnet-0f36b1004dfb340b9",
                "subnet-0b1492f8e65154a9",
                "subnet-0c47184b39a0c58a0"
            ]
        },
        "tst": {
            "securityGroupIds": [
                "sg-0605491d847dae989",
                "sg-0de9d2e400381872b",
                "sg-0276dc6b9ea53ce67",
                "sg-02405d87f5a34c167",
                "sg-04889874abb301580"
            ],
            "subnetIds": [
                "subnet-0f4481e7a214e774a",
                "subnet-03e4e31fa2c8e1618",
                "subnet-0662ffb480a7b1021"
            ]
        }
    },
    "emr_timeouts": {"dev": 15, "tst": 15, "mdl": 15, "prd": 15},
    "jar_location": """f"s3://ta-individual-findw-{env}-codedeployment/miscellaneous/jars/"""",
    "jars": ['ojdbc11.jar', 'postgresql-42.2.23.jar', 'ojdbc11.jar', 'jtds-1.3.1.jar'],
    "tags": """{
        "Name": "appname",
        "TerraformManaged": "false",
        "PrimaryLOB": "Individual",
        "ResourceContact": "tatechdataengineering-dwops@transamerica.com",
        "SecondaryResourceContact": "vignesh.guntu@transamerica.com",
        "Project": "RTS-1333",
        "BillingCostCenter": "0701-PT215013 Digital Platform",
        "AGTManaged": "false",
        "ResourcePurpose": "FTA FINDW {env} resources",
        "Environment": "env",
        "Application": "CIB02456118:FINANCE DATA WARE HOUSE",
        "Division": "transamerica"
        "Channel": "transamerica.individual",
        "applicationType": applicationType,
        "emrSize": emr_size,
        "map-migrated": "d-server-01qco2ca09upii"}""",
     "emr_config": {
        "architecture": "X86_64",
        "autoStartConfiguration": {"enabled": True},
        "autoStopConfiguration": {"enabled": True, "idleTimeoutMinutes": idleTimeoutMinutes},
        "initialCapacity": initialCapacity,
        "maximumCapacity": maximumCapacity,
        "monitoringConfiguration": {"managedPersistenceMonitoringConfiguration": {"enabled": True},
                                   "s3MonitoringConfiguration": {"logUri": "s3://ta-individual-findw-{env}-logs/emr_serverless/"}},
        "networkConfiguration": networkConfiguration,
        "releaseLabel": "emr-7.10.0",
        "runtimeConfiguration": [
            {"classification": "spark",
             "properties": {"maximizeResourceAllocation": "true"}},
            {"classification": "spark-defaults",
             "properties": {"spark.port.maxRetries": "100",
                            "spark.sql.files.maxRecordsPerFile": "1600000",
                            "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
                            "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
                            "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
                            "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
                            "spark.sql.parquet.writeLegacyFormat": "true"}},
            {"classification": "hive-site",
             "properties": {"hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                            "hive.metastore.warehouse.dir": "s3://ta-individual-findw-{env}-logs/emr_serverless/hive_warehouse",
                            "hive.exec.scratchdir": "s3://ta-individual-findw-{env}-logs/emr_serverless/hive_scratch"}},
            {"classification": "emrfs-site",
             "properties": {"fs.s3.maxConnections": "2000",
                            "fs.s3.maxRetries": "200",
                            "fs.s3a.connection.maximum": "2000",
                            "fs.s3a.threads.max": "100"}}
        ],
        "type": applicationType,
        "tags": tags
    }
}
