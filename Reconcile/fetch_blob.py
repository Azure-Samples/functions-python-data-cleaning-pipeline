import logging
import os
import collections
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import StringIO
#kill $(lsof -t -i :7071)

blob_account_name = os.getenv("BlobAccountName")
blob_account_key = os.getenv("BlobAccountKey")
blob_service_client = BlobServiceClient(
    account_url=f"https://{blob_account_name}.blob.core.windows.net",
    credential={"account_name": f"{blob_account_name}", "account_key":f"{blob_account_key}"}
    )

def blob_dict_to_df(my_ordered_dict, filter_string):
    logging.warning('blob_dict_to_df')
    filtered_dict = {k:v for k,v in my_ordered_dict.items() if filter_string in k}
    logging.warning(filtered_dict)
    container_key = list(filtered_dict.keys())[0]
    latest_file = list(filtered_dict.values())[0]
    container_client = blob_service_client.get_container_client(container=container_key)
    blobstring = container_client.download_blob(blob=latest_file).readall().decode('UTF-8')
    df = pd.read_csv(StringIO(blobstring),dtype=str)
    return df

def blob_to_dict(batchId,*args):
    # add containers to list
    container_list = []
    arg_len = (len(args))
    i = 0
    for i in range(arg_len):
        container_list.append(args[i])
        ''.join([str(i) for i in container_list])
    logging.info(container_list)
    # get blob file names from container... azure SDK returns a generator object
    ii = 0
    file_names = []
    for container in container_list:
        logging.warning('FOR LOOP')
        container_client = blob_service_client.get_container_client(container)
        generator = container_client.list_blobs()
        logging.warning(list(generator))
        generator = container_client.list_blobs()
        for file in generator:
            if "cleaned" in file.name:
                file_names.append(file.name)
                ii = ii+1
    # Merge the two lists to create a dictionary
    # container_file_dict  = collections.OrderedDict()
    # container_file_dict = dict(zip(container_list,file_names))
    c1_list = [f for f in file_names if batchId + "_c1" in f]
    c2_list = [f for f in file_names if batchId + "_c2" in f]

    for c in container_list:
        if "c1" in c:
            c1_name = c
        else:
            c2_name = c
    container_file_dict = {}
    container_file_dict[c1_name] = c1_list[0]
    container_file_dict[c2_name] = c2_list[0]
    return container_file_dict
