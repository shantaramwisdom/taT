# Usage 1: python3 idl_crawler.py -i e tst -s p5 -t "clprodsegmentdb"
# Usage 2: python3 idl_crawler.py -e tst -s p65
# Usage 3: python3 idl_crawler.py -e tst -s p65 -r
# Usage 4: python3 idl_crawler.py -e tst -s p65 -r -i
# Usage 5: python3 idl_crawler.py -e tst -s p65 -f monthly
# Usage 6: python3 idl_crawler.py -e tst -s p65
# Usage 7: python3 idl_crawler.py -e tst -s p65 -t "clprodsegmentdb"

import sys
import boto3
import argparse
from botocore.config import Config
import time
import traceback
import logging
config = Config(
    retries = {
        'max_attempts': 10,
        'mode': 'adaptive'
    }
)
glue_client = boto3.client('glue', region_name='us-east-1', config=config)
s3_client = boto3.client('s3', region_name='us-east-1', config=config)
#Update Crawler With S3 Paths
def update_crawler(crawler_name, s3_path_list, recrawl=False):
    try:
        if recrawl:
            glue_client.update_crawler(Name=crawler_name,
            Targets={'S3Targets': s3_path_list},
            RecrawlPolicy={'RecrawlBehavior': 'CRAWL_EVERYTHING'},
            SchemaChangePolicy={'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'LOG'
            })
            log.info(f"Recrawl set to True for crawler {crawler_name}")
        else:
            glue_client.update_crawler(Name=crawler_name,
            Targets={'S3Targets': s3_path_list},
            RecrawlPolicy={'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'},
            SchemaChangePolicy={'UpdateBehavior': 'LOG',
            'DeleteBehavior': 'LOG'
            })
            log.info(f"Recrawl set to False for crawler {crawler_name}")
    except Exception as e:
        raise Exception(f"Error in updating crawler: {e}")
#Start Crawler
def start_crawler(crawler_name):
    try:
        glue_client.start_crawler(Name=crawler_name)
        wait_until_ready(crawler_name)
    except Exception as e:
        raise Exception(f"Error in start crawler {crawler_name} : {e}")
#Define
def get_subfolders_list(bucket_name, prefix, skip_indw=False):
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    subfolders = []
    for page in page_iterator:
        if 'CommonPrefixes' in page:
            for common_prefix in page['CommonPrefixes']:
                if skip_indw and common_prefix['Prefix'].split('/')[3].startswith('indw'):
                    continue
                subfolders.append(common_prefix['Prefix'])
    return subfolders
def get_s3_paths(bucket_name, prefix, skip_indw=False, tables_list=None):
    try:
        if not tables_list:
            subfolders = get_subfolders_list(bucket_name, prefix, skip_indw)
        else:
            if skip_indw:
                tables_list = [table for table in tables_list if not table.startswith('indw')]
            subfolders = [f"{prefix}{table}/" for table in tables_list]
        if not subfolders:
            raise Exception(f"No subfolders found in {bucket_name}/{prefix}")
        s3_path_list = [{
            'Path': f's3://{bucket_name}/{prefix}',
            'Exclusions': [],
            'SampleSize': 240
        } for prefix in subfolders]
        return s3_path_list
    except Exception as e:
        raise Exception(f"Error in getting s3 paths - {e}")
#Wait fro Crawler to be Ready
def wait_until_ready(crawler_name, sleep_seconds=60):
    try:
        while True:
            response = glue_client.get_crawler(Name=crawler_name)
            state = response["Crawler"]["State"]
            log.info(f"Crawler State is: {state}")
            if state == "READY":
                return True
            time.sleep(sleep_seconds)
    except Exception as e:
        raise Exception(f"Crawler {crawler_name}'s Run Failed with {e}.")
#Create Crawler
def create_crawler(crawler_name, crawler_role, db_name, s3_path_list, tags):
    try:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=crawler_role,
            DatabaseName=db_name,
            TablePrefix='',
            Targets={
                'S3Targets': s3_path_list
            },
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'LOG'
            },
            RecrawlPolicy={'RecrawlBehavior': 'CRAWL_EVERYTHING'},
            LineageConfiguration={'CrawlerLineageSettings': 'DISABLE'},
            Tags=tags
        )
        log.info(f"Crawler {crawler_name} created successfully.")
    except Exception as e:
        raise Exception(f"Error while Creating Crawler {crawler_name}. Errored with {e}.")
#Delete Crawler
def delete_crawler(crawler_name):
    try:
        glue_client.delete_crawler(
            Name=crawler_name)
        log.info(f"Crawler {crawler_name} Deleted successfully.")
    except Exception as e:
        raise Exception(f"Error while Deleting Crawler {crawler_name}. Errored with {e}.")
#Check for Crawler Status
def check_crawler_status(crawler_name):
    try:
        crawler = glue_client.get_crawler(Name=crawler_name)
        if crawler:
            return crawler
    except Exception as e:
        log.warning(f"Crawler {crawler_name} doesn't exist.")
        return False
if __name__ == "__main__":
    try:
        log = logging.getLogger(__name__)
        _h = logging.StreamHandler()
        _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s Crawler: %(msg)s", datefmt='%Y-%m-%d %H:%M:%S'))
        log.addHandler(_h)
        log.setLevel(logging.INFO)
        parser = argparse.ArgumentParser()
        parser.add_argument('-s','--source_system', required=True, type=str, dest='source_system', choices=['p5', 'p6', 'p65', 'p75', 'sp1'])
        parser.add_argument('-e','--environment', required=True, type=str, dest='environment', choices=['dev', 'tst', 'mdl', 'prd'])
                parser.add_argument('-f','--frequency', required=False, type=str, dest='frequency', choices=['DAILY', 'MONTHLY'])
        parser.add_argument('-t','--tables', required=False, type=str, dest='tables', default='')
        parser.add_argument('-i','--skip_indw', action='store_true', dest='skip_indw', default=False)
        parser.add_argument('-r','--recrawl', action='store_true', dest='recrawl', default=False)
        args = parser.parse_args()
        source_system = args.source_system
        environment = args.environment
        frequency = args.frequency
        tables = args.tables
        skip_indw = args.skip_indw
        tables_list = tables.split(',')
        tables_list = [i.strip() for i in tables_list if i.strip()]
        tables_list = list(set(tables_list))
        recrawl = args.recrawl
        source_system_name = f'vantage{source_system}'
        if source_system == 'sp1':
            source_system_name = 'vantagep65sp1'
        frequency_suffix = f'{frequency.lower()}' if frequency else ''
        crawler_name = f'ta-individual-findw-{environment}-{source_system_name}-datastage{frequency_suffix}'
        db_name = f'ta-individual-datalake-{environment}-{source_system_name}-curated'
        crawler_role = f'ta-individual-findw-{environment}-data-catalog'
        bucket_name = f'ta-individual-findw-{environment}-datastage'
        prefix = f'financedw/enterprisedatacleansed/{source_system}/'
        tags = {
            "Application": "CIO02456118:FINANCE DATA WARE HOUSE",
            "PrimaryLOB": "Individual",
            "InitiativeType": "RTS",
            "InitiativeId": "RTS-1333",
            "ResourceContact": "tatechdataengineering-dwops@transamerica.com",
            "BillingCostCenter": "0701-PI215013 Digital Platform",
            "AGTManaged": "false",
            "Channel": "transamerica.individual",
            "ResourcePurpose": f"TA FINDW {environment} resources",
            "TerraformManaged": "false",
            "Environment": f"{environment}",
            "Division": "transamerica"
        }
        if not skip_indw:
            log.info("Not Skipping indw tables")
        s3_path_list = get_s3_paths(bucket_name, prefix, skip_indw, tables_list)
        crawler = check_crawler_status(crawler_name)
        if crawler:
            existing_paths = crawler.get('Crawler', {}).get('Targets', {}).get('S3Targets', []) or []
            new_paths_set = {path['Path'] if isinstance(path, dict) else path for path in s3_path_list}
            existing_paths_set = {path['Path'] if isinstance(path, dict) else path for path in existing_paths}
            if not recrawl and not new_paths_set == existing_paths_set:
                added_paths = new_paths_set - existing_paths_set
                removed_paths = existing_paths_set - new_paths_set
                if added_paths:
                    log.info(f"New paths to be added: {added_paths}")
                if removed_paths:
                    log.info(f"Paths to be removed: {removed_paths}")
                log.info(f"Updating crawler {crawler_name} with new paths")
                recrawl = True
            try:
                update_crawler(crawler_name, s3_path_list, recrawl)
            except Exception as ex:
                log.error(f"Error in updating crawler {crawler_name} - {ex}")
                update_crawler(crawler_name, s3_path_list, True)
            #delete_crawler(crawler_name)
            #create_crawler(crawler_name, crawler_role, db_name, s3_path_List, tags)
        else:
            create_crawler(crawler_name, crawler_role, db_name, s3_path_list, tags)
        start_crawler(crawler_name)
    except Exception as e:
        traceback.print_exc()
        log.exception(f"Failing the script due to following Exception {e}")
        sys.exit(1)
    finally:
        log.info("Script completed")

