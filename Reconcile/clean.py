import logging
import os
import pandas as pd
from azure.storage.blob import ContentSettings
from azure.storage.blob import BlockBlobService
from io import StringIO
from . import fetch_blob as fetching_service

blob_account_name = os.getenv("BlobAccountName")
blob_account_key = os.getenv("BlobAccountKey")
block_blob_service = BlockBlobService(account_name=blob_account_name,
                                      account_key=blob_account_key)
out_blob_container_name = os.getenv("FINAL")

# Clean blob flow from event grid events
# This function will call all the other functions in clean.py


def clean(file_1_url, file_2_url, batch_id):
    f1_container = file_1_url.rsplit('/', 2)[-2]
    f2_container = file_2_url.rsplit('/', 2)[-2]
    f2_df, f1_df = fetch_blobs(batch_id, f2_container, f1_container)
    result = final_reconciliation(f2_df, f1_df, batch_id)
    return 'Success'


def fetch_blobs(batch_id, file_2_container_name, file_1_container_name):
    # Create container & blob dictionary with helper function
    blob_dict = fetching_service.blob_to_dict(
        batch_id, file_2_container_name, file_1_container_name)

    # Create F1 DF
    filter_string = 'c1'
    f1_df = fetching_service.blob_dict_to_df(blob_dict, filter_string)

    # Create F2 df
    filter_string = 'c2'
    f2_df = fetching_service.blob_dict_to_df(blob_dict, filter_string)
    return f2_df, f1_df


def final_reconciliation(f2_df, f1_df, batch_id):
    outcsv = f2_df.to_csv(index=False)
    cleaned_blob_file_name = "reconciled_" + batch_id
    block_blob_service.create_blob_from_text(
        out_blob_container_name, cleaned_blob_file_name, outcsv)
    return "Success"
