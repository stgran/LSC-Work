# utils/aws_lambda/functions/create_athena_table/lambda_function.py

import os

import awswrangler as wr
import boto3
import pandas as pd
from loguru import logger

from c2dp.monitoring.sentry import AD_HOC_DATA_INGESTION_DSN, load_sentry
from utils.aws_lambda.functions.create_athena_table.schema_error_handling import (
    compare_existing_schema, compare_input_schema, replace_line_breaks,
    try_read_csv, replace_spaces_with_underscores)

load_sentry(sentry_dsn = AD_HOC_DATA_INGESTION_DSN)

dest_csv_path = os.getenv('DEST_CSV_PATH', default='athena') # default for testing individual methods
GLUE_DATABASE = os.getenv('GLUE_DATABASE', default='c2dp') # default for testing individual methods

client = boto3.client('sts') # aws security token service
sess = boto3.Session(region_name='us-east-1')

s3_client = boto3.client('s3', region_name='us-east-1')
athena_client = boto3.client('athena', region_name='us-east-1')

caller_id = client.get_caller_identity()
account_id = caller_id['Account']
region = os.environ['AWS_REGION']


@logger.catch(reraise=True)
def generate_columns_types(df: pd.DataFrame):
    """
    generate the column types dictionary for wr.catalog.create_csv_table()
    All columns are cast as strings.

    """
    column_names = list(df.columns)
    columns_types = {_name: 'string' for _name in column_names}
    return columns_types


@logger.catch(reraise=True)
def check_schemas(df, bucket, object):
    '''
    This function performs the two main checks:
    - compare_input_schema: Do the column names of the new file match the column names of the oldest file
        in the directory?
    - compare_existing_schema: Do the column names of the files already in the directory match the column
        names of the oldest file in the directory?
    
    These checks are performed twice: first, on the staging directory. If those checks pass, all spaces in
    the column names of the new file are replaced with underscores. Then the checks are performed again on
    the prod directory.
    '''
    
    new_schema = list(df.columns)

    # Compare column names of new object and existing objects in staging directory
    object_name = os.path.basename(object)
    path_name = os.path.dirname(object)
    compare_input_schema(new_schema = new_schema, new_object = object_name, bucket = bucket, path = path_name, staging = True)
    compare_existing_schema(new_object = object_name, bucket = bucket, path = path_name)

    # Replace spaces with underscores in column names
    df = replace_spaces_with_underscores(df)
    new_schema = list(df.columns)
    logger.info(new_schema)

    # Compare column names of new object and existing objects in destination directory
    destination_path_name = f'{dest_csv_path}/{path_name}'
    compare_input_schema(new_schema = new_schema, new_object = object_name, bucket = bucket, path = destination_path_name, staging = False)
    compare_existing_schema(new_object = object_name, bucket = bucket, path = destination_path_name)
    return df


@logger.catch(reraise=True)
def generate_table(database, table, schema, path):
    '''
    Creates a csv table in AWS Glue
    '''
    
    _table = f"src_{table}".replace('-', '_')

    logger.info(f'creating or updating table: {database}.{_table}')

    wr.catalog.create_csv_table(
        database=database,
        table=_table,
        path=path,
        columns_types=schema,
        boto3_session=sess,
        skip_header_line_count=1,
        mode='overwrite',
        serde_library = 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    )


@logger.catch(reraise=True)
def make_table(bucket, object, database=GLUE_DATABASE):
    '''
    This function completes all three main steps that happen when processing a new file.
    1. check_schemas: Ensures that the column names of the new file match the existing column names.
    2. wr.s3.to_csv: Copies the CSV from the staging directory to the prod directory.
    3. generate_table: Creates or updates an AWS Glue table.
    '''

    if object.endswith('/'): # skip over folders
        logger.info('this is a folder, not a file, so we will skip')
        return {'result': 'pass'}

    # We work exclusively with CSVs for ad hoc data
    assert object.endswith('.csv'), f'New object {object} is not a CSV file'

    df = try_read_csv(f's3://{bucket}/{object}')
    df = replace_line_breaks(df) # new line characters can cause issues in Athena tables

    # All checks happen when the object arrives in the staging directory,
    # before it is moved to the prod directory and added to the Glue table
    df = check_schemas(df, bucket, object)

    new_file = f's3://{bucket}/{dest_csv_path}/{object}'
    logger.info(f'writing to s3: {new_file}')
    # Copy object into prod directory
    wr.s3.to_csv(df, path=new_file, index=False)
    
    # create a table based on the new file
    _columns_types = generate_columns_types(df)
    logger.info(f'table columns types: {_columns_types}')
    generate_table(
        database=database,
        table=os.path.basename(new_file).replace('.csv', ''),
        schema=_columns_types,
        path=os.path.dirname(new_file),
        project='c2dp'
    )

    return {'result': 'success'}


@logger.catch(reraise=True)
def lambda_handler(event, context):
    """
    when a new csv arrives in a staging directory on S3, this handler duplicates the csv
    in the respective prod directory and then creates a table in AWS Glue
    """
    logger.info(f'event: {event}')
    for record in [event]:
        logger.info(f'record: {record}')
        _bucket = record['detail']['bucket']['name']
        _object = record['detail']['object']['key']

        make_table(_bucket, _object) # main function