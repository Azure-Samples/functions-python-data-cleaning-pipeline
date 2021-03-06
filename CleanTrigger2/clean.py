import logging
import os
import pandas as pd
from azure.storage.blob import ContentSettings
from azure.storage.blob import BlockBlobService
from io import StringIO

blob_account_name = os.getenv("BlobAccountName")
blob_account_key = os.getenv("BlobAccountKey")
block_blob_service = BlockBlobService(account_name=blob_account_name,
                                      account_key=blob_account_key)
out_blob_container_name = os.getenv("C2")

def clean(req_body):
    blob_obj,filename = extract_blob_props(req_body[0]['data']['url'])
    df = pd.read_csv(StringIO(blob_obj.content))
    result = clean_blob(df, filename)
    return result

def extract_blob_props(url):
    blob_file_name = url.rsplit('/',1)[-1]
    in_container_name = url.rsplit('/',2)[-2]
    readblob = block_blob_service.get_blob_to_text(in_container_name,blob_file_name)                       
    return readblob, blob_file_name

def clean_blob(df, blob_file_name):
    # group by names and item and sum the sales and units
    df1 = df.groupby(["names","item"],as_index=False)[["units","price"]].sum().reset_index()

    # pick one region based on request
    df2 = df1[df1["item"] == 'binder']
    outcsv = df2.to_csv(index=False)

    cleaned_blob_file_name = "cleaned_" +blob_file_name
    block_blob_service.create_blob_from_text(out_blob_container_name, cleaned_blob_file_name, outcsv)
    return "Success"
   
