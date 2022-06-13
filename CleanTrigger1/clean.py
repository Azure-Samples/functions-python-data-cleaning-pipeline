import logging
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import StringIO

blob_account_name = os.getenv("BlobAccountName")
blob_account_key = os.getenv("BlobAccountKey")
blob_service_client = BlobServiceClient(
    account_url=f"https://{blob_account_name}.blob.core.windows.net",
    credential={"account_name": f"{blob_account_name}", "account_key":f"{blob_account_key}"}
    )
out_blob_container_name = os.getenv("C1")

def clean(req_body):
    blob_obj,filename = extract_blob_props(req_body[0]['data']['url']  )
    df = pd.read_csv(StringIO(blob_obj.readall().decode('UTF-8')))
    result = clean_blob(df,filename)
    return result

# Extract blob container name and blob file
def extract_blob_props(url):
  
    blob_file_name = url.rsplit('/',1)[-1]
    in_container_name = url.rsplit('/',2)[-2]

    # remove file extension from blob name
    container_client = blob_service_client.get_container_client(container=in_container_name)
    readblob = container_client.download_blob(blob=blob_file_name)
    return readblob, blob_file_name

def clean_blob(df, blob_file_name):
    
    # group by names and region and sum the sales and units
    df1 = df.groupby(["names","region"],as_index=False)[["units","price"]].sum().reset_index()

    # pick one region based on request
    df2 = df1[df1["region"] == 'east']
    outcsv = df2.to_csv(index=False)

    cleaned_blob_file_name = "cleaned_" +blob_file_name
    container_client = blob_service_client.get_container_client(container=out_blob_container_name)
    container_client.upload_blob(cleaned_blob_file_name, outcsv)
    return "Success"
