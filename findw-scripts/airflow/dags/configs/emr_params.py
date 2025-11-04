emr_config = """{
    "Name": "fta-individual-findw-{env}-ondemand-emr-{source_system}-{emr_name}-{project}-{timestmp}",
    "step_concurrency_level": 15,
    "LogUri": "s3://fta-individual-findw-{env}-logs/emr/",
    "ReleaseLabel": "emr-7.9.0",
    "applications": ["Tez", "Spark", "Hive", "Hadoop"],
    "Keep_Alive": "True",
    "EbsRootVolumeSize": 100,
    "StepConcurrencyLevel": 75,
    "Ec2KeyName": {
        "dev": "ta-individual-findw",
        "tst": "ta-individual-findw",
        "mdl": "ta-prod-support-mdl",
        "prd": "ta-prod-support-mdl"
    },
    "JobFlowRole": "fta-individual-findw-{env}-emr-ec2-role",
    "ServiceRole": "fta-individual-findw-{env}-emr",
    "AutoScalingRole": "fta-individual-findw-{env}-emr-autoscale",
    "SecurityConfiguration": "fta-individual-findw-{env}-sec-config",
    "EmrManagedMasterSecurityGroup": {
        "dev": "sg-0605491d847ae9899",
        "tst": "sg-0b8491d927b945111",
        "mdl": "sg-0804910920745a511",
        "prd": "sg-00e4daa358a37539a"
    },
    "EmrManagedSlaveSecurityGroup": {
        "dev": "sg-0de9d2e400381872b",
        "tst": "sg-0de9d2e400381872b",
        "mdl": "sg-015397419d9821636",
        "prd": "sg-092e89129d88bcbbe"
    },
    "ServiceAccessSecurityGroup": {
        "dev": "sg-0276dc6b9ea53ce647",
        "tst": "sg-0276dc6b9ea53ce647",
        "mdl": "sg-02532164d245ed4f4",
        "prd": "sg-0f19219bf1edd347b"
    },
    "AdditionalSlaveSecurityGroups": {
        "dev": ["sg-02405d87f5a34c167", "sg-04889874abb301580", "sg-0b7e1d4794c864600"],
        "tst": ["sg-02405d87f5a34c167", "sg-04889874abb301580", "sg-0b7e1d4794c864600"],
        "mdl": ["sg-070cc0e4895691769", "sg-0840449d1d24e51ce", "sg-0723b414f73bb25e1"],
        "prd": ["sg-01baf703b6981053", "sg-004d74a0983fa13a3", "sg-0b1d3517fbc3fd8f8"]
    },
    "AdditionalMasterSecurityGroups": {
        "dev": ["sg-02405d87f5a34c167", "sg-04889874abb301580", "sg-0b7e1d4794c864600"],
        "tst": ["sg-02405d87f5a34c167", "sg-04889874abb301580", "sg-0b7e1d4794c864600"],
        "mdl": ["sg-070cc0e4895691769", "sg-0840449d1d24e51ce", "sg-0723b414f73bb25e1"],
        "prd": ["sg-01baf703b6981053", "sg-004d74a0983fa13a3", "sg-0b1d3517fbc3fd8f8"]
    },
    "Ec2SubnetIds": {
        "dev": [
            "subnet-0f4481e7a214e774a",
            "subnet-03e4e31fa2c8e1618",
            "subnet-0662ffb480a7b1021"
        ],
        "tst": [
            "subnet-0f4481e7a214e774a",
            "subnet-03e4e31fa2c8e1618",
            "subnet-0662ffb480a7b1021"
        ],
        "mdl": [
            "subnet-038947a53634af253",
            "subnet-061a123fd16261202",
            "subnet-04ef5a6123539be50"
        ],
        "prd": [
            "subnet-0f36b1004dfb340b9",
            "subnet-0b1492f8e865154a9",
            "subnet-0c47184b39a0c58a0"
        ]
    },
    "InstanceFleets": {
        "small": [
            {
                "Name": "fta-individual-findw-{env}-{project}-emr-master",
                "InstanceFleetType": "MASTER",
                "TargetOnDemandCapacity": 1,
                "TargetSpotCapacity": 0,
                "InstanceTypeConfigs": [{"InstanceType": "m5.4xlarge"}]
            },
            {
                "Name": "fta-individual-findw-{env}-{project}-emr-core",
                "InstanceFleetType": "CORE",
                "TargetOnDemandCapacity": 5,
                "InstanceTypeConfigs": [
                    {"InstanceType": "m5.4xlarge", "WeightedCapacity": 2},
                    {"InstanceType": "m5.8xlarge", "WeightedCapacity": 4}
                ]
            },
            {
                "Name": "fta-individual-findw-{env}-{project}-emr-tsk",
                "InstanceFleetType": "TASK",
                "TargetOnDemandCapacity": 2,
                "InstanceTypeConfigs": [
                    {"InstanceType": "m5.4xlarge", "WeightedCapacity": 2},
                    {"InstanceType": "m5.8xlarge", "WeightedCapacity": 4}
                ]
            }
        ],
        "medium": [
            {
                "Name": "fta-individual-findw-{env}-{project}-emr-master",
                "InstanceFleetType": "MASTER",
                "TargetOnDemandCapacity": 1,
                "TargetSpotCapacity": 0,
                "InstanceTypeConfigs": [{"InstanceType": "m5.24xlarge"}]
            },
            {
                "Name": "fta-individual-findw-{env}-{project}-emr-core",
                "InstanceFleetType": "CORE",
                "TargetOnDemandCapacity": 10,
                "InstanceTypeConfigs": [
                    {"InstanceType": "m5.4xlarge", "WeightedCapacity": 2},
                    {"InstanceType": "m5.8xlarge", "WeightedCapacity": 4},
                    {"InstanceType": "m5.12xlarge", "WeightedCapacity": 6},
                    {"InstanceType": "m5.16xlarge", "WeightedCapacity": 8},
                    {"InstanceType": "m5.24xlarge", "WeightedCapacity": 12}
                ]
            },
            {
                "Name": "fta-individual-findw-{env}-{project}-emr-task",
                "InstanceFleetType": "TASK",
                "TargetOnDemandCapacity": 2,
                "InstanceTypeConfigs": [
                    {"InstanceType": "m5.4xlarge", "WeightedCapacity": 2},
                    {"InstanceType": "m5.8xlarge", "WeightedCapacity": 4},
                    {"InstanceType": "m5.12xlarge", "WeightedCapacity": 6},
                    {"InstanceType": "m5.16xlarge", "WeightedCapacity": 8},
                    {"InstanceType": "m5.24xlarge", "WeightedCapacity": 12}
                ]
            }
        ],
        "large": [
            {
                "Name": "fta-individual-findw-{env}-{project}-emr-master",
                "InstanceFleetType": "MASTER",
                "TargetOnDemandCapacity": 1,
                "TargetSpotCapacity": 0,
                "InstanceTypeConfigs": [{"InstanceType": "r6i.16xlarge"}]
            },
            {
                "Name": "fta-individual-findw-{env}-{project}-emr-core",
                "InstanceFleetType": "CORE",
                "TargetOnDemandCapacity": 20,
                "InstanceTypeConfigs": [
                    {"InstanceType": "r6i.8xlarge", "WeightedCapacity": 4},
                    {"InstanceType": "r6i.16xlarge", "WeightedCapacity": 8},
                    {"InstanceType": "r6i.24xlarge", "WeightedCapacity": 12},
                    {"InstanceType": "r6i.32xlarge", "WeightedCapacity": 16},
                    {"InstanceType": "r5.24xlarge", "WeightedCapacity": 12}
                ]
            },
            {
                "Name": "fta-individual-findw-{env}-{project}-emr-task",
                "InstanceFleetType": "TASK",
                "TargetOnDemandCapacity": 4,
                "InstanceTypeConfigs": [
                    {"InstanceType": "r6i.8xlarge", "WeightedCapacity": 4},
                    {"InstanceType": "r6i.16xlarge", "WeightedCapacity": 8},
                    {"InstanceType": "r6i.24xlarge", "WeightedCapacity": 12},
                    {"InstanceType": "r6i.32xlarge", "WeightedCapacity": 16},
                    {"InstanceType": "r5.24xlarge", "WeightedCapacity": 12}
                ]
            }
        ]
    }
    },
    "ManagedScalingPolicy": {
        "small": {
            "ComputeLimits": {
                "UnitType": "InstanceFleetUnits",
                "MaximumOnDemandCapacityUnits": 60,
                "MaximumCapacityUnits": 60,
                "MinimumCapacityUnits": 4,
                "MaximumCoreCapacityUnits": 10
            }
        },
        "medium": {
            "ComputeLimits": {
                "UnitType": "InstanceFleetUnits",
                "MaximumOnDemandCapacityUnits": 120,
                "MaximumCapacityUnits": 120,
                "MinimumCapacityUnits": 24,
                "MaximumCoreCapacityUnits": 20
            }
        },
        "large": {
            "ComputeLimits": {
                "UnitType": "InstanceFleetUnits",
                "MaximumOnDemandCapacityUnits": 240,
                "MaximumCapacityUnits": 240,
                "MinimumCapacityUnits": 24,
                "MaximumCoreCapacityUnits": 40
            }
        }
    },
    "Configurations": [
        {
            "Classification": "spark",
            "Properties": {"maximizeResourceAllocation": "true"}
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
                "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
                "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
                "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
                "spark.sql.parquet.writeLegacyFormat": "true",
                "spark.sql.files.maxRecordsPerFile": "1600000",
                "spark.port.maxRetries": "100"
            }
        },
        {
            "configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}
                }
            ],
            "Classification": "spark-env",
            "Properties": {}
        },
        {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.server2.session.check.interval": "3600000",
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                "hive.server2.idle.operation.timeout": "7200000",
                "hive.cluster.delegation.token.store.class": "org.apache.hadoop.hive.thrift.DBTokenStore",
                "hive.server2.idle.session.timeout": "21600000"
            }
        },
        {
            "Classification": "yarn-site",
            "Properties": {
                "yarn.resourcemanager.scheduler.class": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler",
                "yarn.resourcemanager.nodemanager-graceful-decommission-timeout-secs": "1800",
                "yarn.scheduler.max.assign": "500",
                "yarn.scheduler.fair.allow-undeclared-pools": "false",
                "yarn.scheduler.fair.assignmultiple": "true",
                "yarn.scheduler.fair.user-as-default-queue": "false"
            }
        },
        {
            "Classification": "hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
        },
        {
            "Classification": "emrfs-site",
            "Properties": {
                "fs.s3.maxRetries": "200",
                "fs.s3.maxConnections": "2000",
                "fs.s3.threads.max": "100",
                "fs.s3a.connection.maximum": "2000"
            }
        },
        {
            "Classification": "oozie-site",
            "Properties": {
                "oozie.https.enabled": "false"
            }
        },
        {
            "Classification": "iceberg-defaults",
            "Properties": {
                "iceberg.enabled": "true"
            }
        }
    ],
    "AutoTerminationPolicy": {
        "dev": {"IdleTimeout": 1800},
        "tst": {"IdleTimeout": 1800},
        "mdl": {"IdleTimeout": 1800},
        "prd": {"IdleTimeout": 1800}
    },
    "BootstrapActions": [
        {
            "Name": "Finance DW EMR BootStrap Action",
            "ScriptBootstrapAction": {
                "Path": "s3://ta-individual-findw-{env}-codedeployment/miscellaneous/bootstrap-action/bootstrap_action.sh",
                "Args": [
                    "{env}",
                    "ta-individual-findw-{env}-common",
                    "findw",
                    "{project}",
                    "{cycle_date}",
                    "N"
                ]
            }
        }
    ],
    "Tags": [
        {"Key": "Name", "Value": "fta-individual-findw-{env}-ondemand-emr-{source_system}-{emr_name}-{project}"},
        {"Key": "TerraformManaged", "Value": "false"},
        {"Key": "PrimaryLOB", "Value": "Individual"},
        {"Key": "ResourceContact", "Value": "tatechdataengineering-dwops@transamerica.com"},
        {"Key": "SecondaryResourceContact", "Value": "vignesh.guntu@transamerica.com"},
        {"Key": "Project", "Value": "RTS-1333"},
        {"Key": "BillingCostCenter", "Value": "0701-PT215013 Digital Platform"},
        {"Key": "AGTManaged", "Value": "false"},
        {"Key": "ResourcePurpose", "Value": "FTA FINDW {env} resources"},
        {"Key": "Environment", "Value": "{env}"},
        {"Key": "Application", "Value": "CIB02456118:FINANCE DATA WARE HOUSE"},
        {"Key": "Division", "Value": "transamerica"},
        {"Key": "Channel", "Value": "transamerica.individual"},
        {"Key": "map-migrated", "Value": "d-server-01qco2ca09upii"}
    ]
}"""
