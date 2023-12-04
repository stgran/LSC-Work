# utils/aws_lambda/functions/create_athena_table/schema_error_handling.py

import awswrangler as wr
import boto3
from loguru import logger

sess = boto3.Session(region_name='us-east-1')
s3_client = boto3.client('s3', region_name='us-east-1')


def try_read_csv(path):
    '''
    Sometimes a UnicodeDecodeError arises when we read_csv. This error seems related to the existence of quotation marks in
    string-heavy CSVs. According to https://stackoverflow.com/questions/61264795/pandas-unicodedecodeerror-utf-8-codec-cant-decode-bytes-in-position-0-1-in
    adding the pandas.read_csv() argument encoding='unicode_escape' may prevent UnicodeDecodeErrors from arising.

    According to the Python docs, the unicode_escape encoding means the following:
    "Encoding suitable as the contents of a Unicode literal in ASCII-encoded Python source code, except that quotes are not escaped.
    Decode from Latin-1 source code. Beware that Python source code actually uses UTF-8 by default."
    '''
    try:
        df = wr.s3.read_csv(path, dtype='str')
        return df
    except UnicodeDecodeError:
        df = wr.s3.read_csv(path, dtype='str', encoding='unicode_escape')
        return df


def replace_line_breaks(df):
    '''
    We replace new line characters ("\n") with spaces because new line characters were causing
    issues with how Athena interpreted new lines, including splitting data across multiple rows
    '''
    out = df.apply(lambda s: s.str.replace('\n', ' ').replace('\r', ''))
    check_output = out.apply(lambda s: s.str.contains(r'\n'))
    assert all(check_output.eq(False)) # check that there are no new line characters in the data after replacement
    return out


def replace_spaces_with_underscores(df):
    '''
    If we are creating a table, AWS Glue changes column names containing spaces so we replace 
    spaces with underscores in column names before we create the table.
    '''
    df.columns = df.columns.str.replace(' ', '_')

    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])
    return df


def get_oldest_schema(bucket: str, path: str):
    '''
    This method identifies the CSV file with the oldest last_modified date in a directory.

    It returns the list of column names of that CSV file.
    '''
    prefix = path + '/'
    logger.info(prefix)

    list_objects_response = s3_client.list_objects(
        Bucket = bucket,
        Prefix = prefix)

    # If no files exist in the directory, we can break because there's no schema to compare to
    if 'Contents' not in list_objects_response:
        return 'No existing files'

    objects_list = list_objects_response['Contents']

    csv_objects_list = [object for object in objects_list if object['Key'].endswith('.csv')]
    oldest_csv_object = min(csv_objects_list, key = lambda x: x['LastModified'])
    oldest_csv_name = oldest_csv_object['Key']

    oldest_csv_df = try_read_csv(f's3://{bucket}/{oldest_csv_name}')
    oldest_csv_schema = list(oldest_csv_df.columns)
    return oldest_csv_schema


def compare_input_schema(new_schema: list, new_object: str, bucket: str, path: str, staging = False):
    '''
    This method compares the column names of the new CSV file to the column names of CSV objects in an S3 directory. This method extracts the schema from the oldest file in the destination location and compares it to the new schema. This method assumes that the oldest file is the single source of truth because all files that were captured afterwards are compared to this oldest file.

    If the column names of the CSV file do not match the column names of the oldest CSV object in the S3 directory,
    this method raises an error.
    '''
    oldest_schema = get_oldest_schema(bucket, path)

    if oldest_schema == 'No existing files':
        return 'No previous schema to compare to'

    logger.info(new_schema)
    logger.info(oldest_schema)

    # 2023-06-14: files in the production location (athena/verification/...) are sanitized (including replacing spaces with undercores);
    # however, the files that land in the staging directory are not necessarily sanitized. for this reason, we need to test whether sanitizing the new file's schema would equal the existing production schema
    if staging:
        assert [x.replace(' ', '_') for x in new_schema] == [x.replace(' ', '_') for x in oldest_schema], f'Column names of {new_object} do not match existing schema in {bucket}/{path}'
    else:
        assert new_schema == oldest_schema, f'Column names of {new_object} do not match existing schema in {bucket}/{path}'
    
    return 'Success: new object schema matches existing schema'


def compare_existing_schema(bucket: str, path: str, new_object: str = ''):
    '''
    This method compares the column names of the existing CSV objects in an S3 directory to the column names of
    the oldest CSV object in the S3 directory.

    If the column names of all existing CSV objects are not identical, in terms of order and names, this method
    raises an error.

    The error names all the CSV objects
    '''
    oldest_schema = get_oldest_schema(bucket, path)

    if oldest_schema == 'No existing files':
        return 'No previous schema to compare to'

    objects_list = wr.s3.list_objects(f's3://{bucket}/{path}/*.csv', boto3_session = sess)

    deviant_objects = []

    assert all(obj.endswith('.csv') for obj in objects_list), f'Object(s) in {path} are not CSV files'

    for object in objects_list:
        if object != f's3://{bucket}/{path}/{new_object}':
            df = wr.s3.read_csv(object, dtype='str')
            column_names = list(df.columns)
            if oldest_schema != column_names:
                deviant_objects.append(object)

    assert len(deviant_objects) == 0, f'Column names of existing objects {deviant_objects} do not match oldest schema in {bucket}/{path}'
    return 'Success: consistent existing schema'
