"""

Data request: name removed

This workflow downloads data from models defined in the (name removed) repository.
This workflow identifies all glue tables that have a certain prefix (which indicates that they are part of this data request)
and then downloads the data locally as csv files and zips them into a zip file.

"""
import datetime
import os
import json
from loguru import logger
import pandas as pd

from prefect import flow
import shutil

# The following imports were functions we used frequently when
# interacting with AWS Athena, Prefect, and Box.
from dependencies.c2dp.aws.athena.identify_request_models import identify_request_models
from dependencies.c2dp.aws.athena.download_data import download_data
from dependencies.utils.box.read_file import (create_client, read_boxfile,
                                              upload_files, list_folder_items)
from dependencies.utils.prefect.load_secret import load_secret

BOX_CLIENT_SECRET = load_secret('box-client-secret')
BOX_CLIENT_ID = load_secret('box-client-id')

# We use the flow config file to name the Prefect Flow
dir_name = os.path.dirname(os.path.abspath(__file__))
with open(f'{dir_name}/flow_config.json') as f:
    FLOW_CONFIG = json.loads(f.read())

model_configs = [
    { # NOTE: removed original config info
        'prefix': 'fake_prefix',
        'included_models': ['fake_prefix_included_model'],
        'folder_id': '0123456789',
        'comparison_folder_id': '0123456789'
},
]

today = datetime.datetime.today()
OUTPUT_ZIP_FILE_NAME = f"""{today.strftime('%Y-%m-%d')}"""

DOWNLOAD_PATH = today.strftime(f'%Y-%m-%d_fake_path')

OUTPUT_FILE = today.strftime(f'%Y-%m-%d_fake_file')

def remove_previous(user_client, config, df):
    '''
    This function removes previously delivered data from the new data delivery.

    First it gathers all the surrogate keys from previous data deliveries.

    After these have been deduplicated, it creates a boolean mask of if surrogate
    keys in the new data appear in the df of previously delivered surrogate keys.

    Based on this boolean mask, it only keeps new surrogate keys which do not appear
    in the df of previously delivered surrogate keys.

    The remaining data is saved, overwriting the new data delivery file pulled from Athena.
    '''
    
    previous_delivery_ids = list_folder_items(client = user_client, folder_id = config['comparison_folder_id'])
    
    number_of_prev_ids = len(previous_delivery_ids)
    logger.info(f'Scanning {number_of_prev_ids} files')

    # Get the surrogate keys of the previously delivered data
    previous_surrogate_keys = [] # TODO : Convert to DF
    
    for previous_delivery in previous_delivery_ids:
        logger.info(f'Scanning file {previous_delivery}')
        previous_df = read_boxfile(client = user_client, file = previous_delivery)
        previous_surrogate_keys.extend(previous_df['Surrogate Key'])
        new_row_count = len(previous_df['Surrogate Key'])
        logger.info(f'Added {new_row_count} previously delivered surrogate keys')
        logger.info(len(previous_surrogate_keys))

    previous_surrogate_keys = pd.DataFrame(columns=['surrogate_key'], data=previous_surrogate_keys)
    logger.info(previous_surrogate_keys)
    previous_surrogate_keys['surrogate_key'] = previous_surrogate_keys['surrogate_key'].drop_duplicates()

    logger.info(previous_surrogate_keys)
    # Remove previously delivered rows by surrogate keys from the current delivery
    mask = df['Surrogate Key'].isin(previous_surrogate_keys['surrogate_key'])
    logger.info(mask)
    
    df_size = df.shape[0]
    logger.info(f'Before removing previous keys, {df_size} rows in df')
    
    df = df[~mask]
    
    df_size = df.shape[0]
    logger.info(f'After removing previous keys, {df_size} rows in df')

    # Resave the current delivery
    output_file = f'{DOWNLOAD_PATH}/{OUTPUT_FILE}.csv'
    df.to_csv(output_file, index=False)

@flow(name=FLOW_CONFIG['flow_name'])
def run_flow():
    '''
    This function prepares the data delivery for [name removed].

    - Queries the relevant Athena table for this data delivery
    - Saves data locally
    - Removes previously delivered data
    - Uploads to staging folder in Box
    '''
    user_client = create_client(
        client_id=BOX_CLIENT_ID,
        client_secret=BOX_CLIENT_SECRET)

    for config in model_configs:
        
        # if there is already a directory created, delete it (this mostly happens during local testing)
        if os.path.exists(DOWNLOAD_PATH): shutil.rmtree(DOWNLOAD_PATH)
        os.makedirs(DOWNLOAD_PATH)

        tables = identify_request_models(
            prefix=config['prefix'], 
            models_to_include=config['included_models']
        )

        for table in tables: # TODO : Change the design to single table
            logger.info(table)

            df = download_data(
                table, 
                download_path=DOWNLOAD_PATH,
                file_name=OUTPUT_FILE,
                order_by='date_filed, case_number',
                capitalize=True
            )
        
            # Remove previously delivered data
            remove_previous(user_client, config, df)
        
        files_to_upload = [{
                'folder_id': config['folder_id'],
                'file_name': [f'{DOWNLOAD_PATH}/{OUTPUT_FILE}.csv']
            }]

        upload_files(files_to_upload)

        logger.info('Removing local download path')
        shutil.rmtree(f'{DOWNLOAD_PATH}')