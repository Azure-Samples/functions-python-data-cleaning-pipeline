import logging
import os
import collections
import pandas as pd
import numpy as np
from azure.storage.blob import ContentSettings
from azure.storage.blob import BlockBlobService
from io import StringIO
#kill $(lsof -t -i :7071)

blob_account_name = os.getenv("BlobAccountName")
blob_account_key = os.getenv("BlobAccountKey")
block_blob_service = BlockBlobService(account_name=blob_account_name,
                                      account_key=blob_account_key)

def blob_dict_to_df(my_ordered_dict, filter_string):
    logging.warning('blob_dict_to_df')
    logging.warning(my_ordered_dict)
    logging.warning(filter_string)
    filtered_dict = {k:v for k,v in my_ordered_dict.items() if filter_string in k}
    logging.warning(filtered_dict)
    container_key = list(filtered_dict.keys())[0]
    latest_file = list(filtered_dict.values())[0]
    blobstring = block_blob_service.get_blob_to_text(container_key, latest_file).content
    df = pd.read_csv(StringIO(blobstring),dtype=str)
    df = df.replace(np.nan, '', regex=True)
    df["initstate"] = df["finalresult"].map(lambda x: "0" if "no" in x else "2")
    #logging.warning(df.head())
    return df

def blob_to_dict(*args):
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
        generator = block_blob_service.list_blobs(container)
        logging.warning(list(generator))
        for file in generator:
            file_names.append(file.name)
            logging.info(file_names[ii]) 
            ii = ii+1
    # Merge the two lists to create a dictionary
    container_file_dict  = collections.OrderedDict()
    container_file_dict = dict(zip(container_list,file_names))
    #blob_dict_to_df(container_file_dict)
    logging.warning('blob_to_dict function')
    logging.warning(container_file_dict)
    return container_file_dict
