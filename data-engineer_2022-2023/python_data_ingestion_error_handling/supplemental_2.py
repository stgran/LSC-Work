# utils/aws_lambda/functions/create_athena_table/utils/clean_bucket.py

import os
import awswrangler as wr
import logging
from loguru import logger
import boto3

from utils.aws_lambda.functions.create_athena_table.schema_error_handling import compare_existing_schema

sess = boto3.Session(region_name='us-east-1')

'''
This script is designed to identify inconsistent schema in directories in the court-data-management AWS S3 bucket.

Each low-level directory (i.e. containing only files and no further directories) feeds into a single AWS Glue table,
so each CSV needs to have identical schema (column names) for the Glue table to accurate reflect all the CSV files
which feed into it.

This script crawls over all the directories in court-data-management (excluding the /test_files directory) to the lowest
level directories. It then uses the schema_error_handling methods to identify directories containing inconsistent schema.

With this information, we can quickly go through these inconsistent directories and decide how to handle the inconsistencies.
'''


def main(output_file: str, bucket, directory: str = None, ignore_prefix_list: list = None):
    '''
    This function passes each directory containing objects through compare_existing_schema.
    If the directory contains other directories, we call this function on each contained directory.
    If the directory contains CSVs, we call schema_error_handling.compare_existing_schema()
    on the directory.
    '''
    dir_name = os.path.dirname(__file__)
    try:
        os.remove(f'{dir_name}/tmp/{output_file}')
    except:
        pass
    logging.basicConfig(level=logging.ERROR, filename=f'{dir_name}/tmp/{output_file}')

    if directory:
        path = f's3://{bucket}/{directory}/'
    else:
        path = f's3://{bucket}/'
    
    object_list = wr.s3.list_objects(path = path, boto3_session = sess)
    
    if ignore_prefix_list:
        for ignore_prefix in ignore_prefix_list:
            ignore_path = f's3://{bucket}/{ignore_prefix}'
            ignore_object_list = wr.s3.list_objects(path = ignore_path, boto3_session = sess)
            object_list = list(set(object_list) - set(ignore_object_list))
    
    non_csv_objects = []
    if not all(obj.endswith('.csv') for obj in object_list):
        non_csv_objects.extend([non_csv_object for non_csv_object in object_list if not non_csv_object.endswith('.csv')])
        logging.exception(f'All files should be CSVs. {non_csv_objects} are not.')
    
    directories_containing_objects = [*set([os.path.dirname(obj) for obj in object_list])]
    
    for directory in directories_containing_objects:
        logger.info(f'Checking {directory}')
        contained_directories = wr.s3.list_directories(path = f'{directory}/', boto3_session = sess)
        if len(contained_directories) > 0:
            logger.info(f'Identified mixed object types in {directory}')
            logging.exception(f'{directory} contains CSVs and directories')
        dir_name = directory.replace(f's3://{bucket}/', '')
        try:
            compare_existing_schema(bucket = bucket, path = dir_name)
        except AssertionError:
            logger.info('Identified inconsistent schema')
            logging.exception(f'Inconsistent schema in {path}')
        except UnicodeDecodeError:
            logger.info(f'Error reading CSV in {path}')
            logging.exception(f'Error reading CSV in {path}')

if __name__ == '__main__':
    main(output_file = 'schema_errors_verification.log', bucket = 'court-data-management', directory = 'verification')