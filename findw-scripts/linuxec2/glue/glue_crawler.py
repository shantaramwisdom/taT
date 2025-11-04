# Purpose: Wrapper for performing glue crawler actions like starting crawler, creating crawler etc,
# Usage: python3 glue_crawlers.py "action" "json_config"
# action: possible values are "create_crawler", "start_crawler", "create_classifier", "update_crawler"
# json_config: json config passed as a string with req. params
# 12/17/2021       Sowjanya J
# LTCG - Initial Development

import boto3
import time
import sys
import json
import os
from botocore.exceptions import ClientError
from botocore.client import Config
#region = os.popen('aws configure get region').read().strip()
config = Config(retries={'max_attempts': 15, 'mode': 'adaptive'})
region = 'us-east-1'
client = boto3.client('glue', region_name=region, config=config)

def update_crawler(**crawler_params):
    try:
        crawler_name = crawler_params['crawler_name']
        update_config = crawler_params['update_config']
        code = 'client.update_crawler(Name=crawler_name, ' + update_config + ')'
        print("code", code)
        exec(code)
        print("**************The crawler {crawler_name} is updated successfully**************")
        print("crawler_status: Success")
    except KeyError as ke:
        print("**************update_config not found, Skipping  update_crawler**************")
    except Exception as e:
        print("**************The following exception occurred while creating the updating the crawler:{crawler_name}", e)
        print("crawler_status: Failed")
        raise e

def delete_crawler(**crawler_params):
    try:
        crawler_name = crawler_params['crawler_name']
        client.delete_crawler(Name=crawler_name)
        print("**************The crawler {crawler_name} is deleted successfully**************")
        print("crawler_status: Success")
    except ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            print("**************The crawler {crawler_name} is not found**************")
            print("crawler_status: Success")
        else:
            print("crawler_status: Failed")
            raise error

def delete_classifier(**crawler_params):
    try:
        classifier_name = crawler_params['classifier_name']
        client.delete_classifier(Name=classifier_name)
        print("**************The classifier {classifier_name} is deleted successfully**************")
        print("crawler_status: Success")
    except ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            print("**************The classifier {classifier_name} is not found**************")
            print("crawler_status: Success")
        else:
            print("crawler_status: Failed")
            raise error

def create_crawler(**crawler_params):
    crawler_role = crawler_params['crawler_role']
    catalog_db_name = crawler_params['catalog_db_name']
    s3_paths = crawler_params['s3_path']
    crawler_name = crawler_params['crawler_name']
    classifier = crawler_params['classifier']
    env_name = crawler_name.split('-')[1]
    tags_str = open('/application/financedw/scripts/glue/config/tags.json', 'r').read()
    tags_str = tags_str.replace('<env>', f'{env_name}').replace('<crawler_name>', f'{crawler_name}')
    tags = json.loads(tags_str)
    try:
        try:
            crawler_response = client.get_crawler(
                Name=crawler_name
            )
            print("**************The crawler ", crawler_name, "is found**************")
            print("crawler_status: Success")
        except ClientError:
            if classifier:
                classifier_string = [classifier, ]
            else:
                classifier_string = []
            s3_target = []
            for s3_path in s3_paths:
                s3_target.append({'Path': s3_path, 'Exclusions': []})
            response = client.create_crawler(
                Name=crawler_name,
                Role=crawler_role,
                DatabaseName=catalog_db_name,
                Targets={'S3Targets': s3_target},
                Classifiers=classifier_string,
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DELETE_FROM_DATABASE'
                },
                RecrawlPolicy={'RecrawlBehavior': 'CRAWL_EVERYTHING'},
                LineageConfiguration={'CrawlerLineageSettings': 'DISABLE'},
                Configuration='{"Version": 1.0,"Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas"}}',
                Tags=tags
            )
            print("**************The crawler ", crawler_name, "is created**************")
            print("crawler_status: Success")
    except Exception as e:
        print("**************The following exception occurred while creating the glue crawler:", e, "**************")
        print("crawler_status: Failed")
        raise e

def create_classifier(**classifier_params):
    classifier_name = classifier_params['classifier_name']
    delimiter = classifier_params['delimiter']
    quote = classifier_params['quote']
    header = classifier_params['header']
    print("header", header)
    try:
        response = client.get_classifier(
            Name=classifier_name
        )
        print("**************The classifier has been found: ", classifier_name, "**************")
        print("crawler_status: Success")
    except ClientError:
        response = client.create_classifier(CsvClassifier={
            'Name': classifier_name,
            'Delimiter': delimiter,
            'QuoteSymbol': quote,
            'ContainsHeader': 'UNKNOWN',
            'Header': header,
            'DisableValueTrimming': False,
            'AllowSingleColumn': False
        })
        print("**************The classifier has been created:", classifier_name, "**************")
        print("crawler_status: Success")
    except Exception as e:
        print("**************The following exception occurred while creating the glue classifier :", e, "**************")
        print("crawler_status: Failed")
        raise e

def start_crawler(**crawler_params):
    try:
        crawler_name = crawler_params['crawler_name']
        response = client.start_crawler(
            Name=crawler_name
        )
        print("**************The crawler ", crawler_name, "has been started**************")
        sense_crawler(crawler_name)
    except Exception as e:
        print("**************The following exception occurred while starting the glue crawler :", e, "**************")
        print("crawler_status: Failed")
        raise e
# Last crawl possible states
def sense_crawler(crawler_name):
    while True:
        response = client.get_crawler(
            Name=crawler_name
        )
        crawler_status = response['Crawler']['State']
        if crawler_status == 'READY':
            run_status = response['Crawler']['LastCrawl']['Status']
            if run_status == 'SUCCEEDED':
                print("**************The crawler ", crawler_name, "run is successful**************")
                print("crawler_status: ", run_status)
                return True
            else:
                print("**************The crawler ", crawler_name, "run is failed**************")
                print("crawler_status: ", run_status)
                return False
        time.sleep(5)

if __name__ == "__main__":
    action = sys.argv[1]
    print("action: ", action)
    params = json.loads(sys.argv[2])
    print("params: ", params)
    globals()[action](**params)