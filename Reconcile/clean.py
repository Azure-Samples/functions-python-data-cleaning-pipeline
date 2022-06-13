import logging
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import StringIO
from . import fetch_blob as fetching_service

blob_account_name = os.getenv("BlobAccountName")
blob_account_key = os.getenv("BlobAccountKey")
blob_service_client = BlobServiceClient(
    account_url=f"https://{blob_account_name}.blob.core.windows.net",
    credential={"account_name": f"{blob_account_name}", "account_key":f"{blob_account_key}"}
    )
out_blob_container_name = os.getenv("FINAL")
container_client = blob_service_client.get_container_client(container=out_blob_container_name)

# Clean blob flow from event grid events
# This function will call all the other functions in clean.py

def clean(file_1_url,file_2_url,batch_id):
    f1_container = file_1_url.rsplit('/', 2)[-2]
    f2_container = file_2_url.rsplit('/', 2)[-2]
    f2_df, f1_df = fetch_blobs(batch_id,f2_container,f1_container)
    result = final_reconciliation(f2_df, f1_df,batch_id)
    return 'Success'

def fetch_blobs(batch_id,file_2_container_name,file_1_container_name):
    # Create container & blob dictionary with helper function
    blob_dict = fetching_service.blob_to_dict(batch_id,file_2_container_name,file_1_container_name)
    
    # Create F1 DF
    filter_string = 'c1'
    f1_df = fetching_service.blob_dict_to_df(blob_dict, filter_string)

    # Create F2 df
    filter_string = 'c2'
    f2_df = fetching_service.blob_dict_to_df(blob_dict, filter_string)
    return f2_df, f1_df

def final_reconciliation(f2_df, f1_df,batch_id):  
    outcsv = f2_df.to_csv(index=False)
    cleaned_blob_file_name = "reconciled_" + batch_id
    container_client.upload_blob(name=cleaned_blob_file_name, data=outcsv)
    return "Success"
