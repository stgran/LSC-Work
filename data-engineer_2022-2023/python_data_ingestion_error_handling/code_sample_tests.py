# utils/aws_lambda/functions/create_athena_table/tests/test_schema_error_handling.py

import pytest

from utils.aws_lambda.functions.create_athena_table.schema_error_handling import (
    compare_existing_schema, compare_input_schema, get_oldest_schema, try_read_csv)


def test_get_oldest_schema():
    '''
    This tests get_oldest_schema with the schema of a specific test file.
    '''
    oldest_schema = get_oldest_schema('court-data-management', 'test_files/misaligned_schema')

    assert oldest_schema == ['year', 'county', 'case_category', 'case_type', 'case_type_code', 'variable', 'value']


def test_compare_input_schema_warning():
    '''
    This tests compare_input_schema with inputs that should result in a warning being raised.
    '''
    new_object = 'fake_data_misaligned_schema.csv'
    bucket = 'court-data-management'
    path = 'test_files/misaligned_schema'

    new_object_path = f's3://{bucket}/{path}/{new_object}'
    new_object_df = try_read_csv(new_object_path)
    new_schema = list(new_object_df.columns)

    with pytest.raises(AssertionError) as record:

        compare_input_schema(
            new_schema = new_schema,
            new_object = new_object,
            bucket = bucket,
            path = path
        )

    assert record.type == AssertionError
    assert str(record.value) == f'Column names of {new_object} do not match existing schema in {bucket}/{path}'


def test_compare_input_schema_no_warning():
    '''
    This tests compare_input_schema with inputs that should result in a success message, with no warning being raised.
    '''
    new_object='fake_data_aligned_schema.csv'
    bucket = 'court-data-management'
    path = 'test_files/aligned_schema'

    new_object_path = f's3://{bucket}/{path}/{new_object}'
    new_object_df = try_read_csv(new_object_path)
    new_schema = list(new_object_df.columns)

    success_message = compare_input_schema(
        new_schema = new_schema,
        new_object = new_object,
        bucket = bucket,
        path = path
    )

    assert success_message == 'Success: new object schema matches existing schema'


def test_compare_existing_schema_warning():
    '''
    This tests compare_existing_schema with inputs that should result in a warning being raised.
    '''
    new_object = 'does_not_exist.csv'
    bucket = 'court-data-management'
    path = 'test_files/misaligned_schema'

    with pytest.raises(AssertionError) as record:

        compare_existing_schema(
            new_object = new_object,
            bucket = bucket,
            path = path
        )

    assert record.type == AssertionError
    assert str(record.value) == f'Column names of existing objects [\'s3://court-data-management/test_files/misaligned_schema/fake_data_misaligned_schema.csv\'] do not match oldest schema in {bucket}/{path}'


def test_compare_existing_schema_no_warning():
    '''
    This tests compare_existing_schema with inputs that should result in a success message, with no error being raised.
    '''
    new_object='does_not_exist.csv'
    bucket = 'court-data-management'
    path = 'test_files/aligned_schema'

    success_message = compare_existing_schema(
        new_object = new_object,
        bucket = bucket,
        path = path
    )

    assert success_message == 'Success: consistent existing schema'