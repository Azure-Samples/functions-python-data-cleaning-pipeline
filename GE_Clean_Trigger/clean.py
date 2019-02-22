import logging
import os
import pandas as pd
from azure.storage.blob import ContentSettings
from azure.storage.blob import BlockBlobService
from io import StringIO


#python3.6 -m venv funcenv... this creates the funcenv
# source funcenv/bin/activate... this activates virtual environment created above
# func host start after each change
# pip install -r requirements.txt
#

blob_account_name = os.getenv("BlobAccountName")
blob_account_key = os.getenv("BlobAccountKey")
block_blob_service = BlockBlobService(account_name=blob_account_name,
                                      account_key=blob_account_key)
out_blob_container_ge_name = os.getenv("OutBlobContainerGEName")
out_blob_pomatch = os.getenv("OutBlobPoMatch")
# Clean blob flow from event grid events
# This function will call all the other functions in clean.py
def clean(req_body):
    blob_obj,filename = extract_blob_props(req_body[0]['data']['url']  )
    logging.info('***************Main Function******************')
    logging.info(filename)
    # This if statement calls the cleaning functions
    # This if statement routes cleaning based on filename to the correct function
    # This if statement will throw an Exception Error if a filename doesn't contain GE or MTU
    #### Need to throw an error if the blobs are empty
    #### Need a guid to ensure that MTU and GE files are paired together.
    result = ge_clean_blob(blob_obj.content, filename)
    return result

# Extract blob container name and blob file
def extract_blob_props(url):
    logging.warning('Extract Blob Testing')
    logging.info('*************URL Below********')
    logging.info(url)
    # rsplit starts from right and then looks for '/' and delimites first word
    logging.info(url.rsplit('/',1))
    logging.info(url.rsplit('/',2))
    #blob file is last word in the list that is generated from url.rsplit
    blob_file_name = url.rsplit('/',1)[-1]
    in_container_name = url.rsplit('/',2)[-2]
    # remove file extension from blob name
    #blob_name_less_csv =  blob_file_name.rsplit('.',1)[-2]
    readblob = block_blob_service.get_blob_to_text(in_container_name,blob_file_name)                       
    return readblob, blob_file_name

#Clean GE File
def ge_clean_blob(content, blob_file_name):
    #fires on http trigger 2
    GE_df = pd.read_csv(StringIO(content))
    logging.warning('GE Cleaning')
    logging.info('BEFORE CLEANING')
    logging.info(GE_df.head(5)) 
    logging.info(len(GE_df.index))
    GE_df['GE_PO_#'] = GE_df['Customer PO #']
    
    #logging.info(content)# Add PO and Price column for GE format
    GE_df['PO_Price_Match'] = (GE_df['Customer PO #'].astype(str)
                                      +"_"+ GE_df['Gross Sales'].astype(str))

    # Add PO, Inv, and Price column for GE format
    GE_df['PO_Inv_Price_Match'] = (GE_df['Customer PO #'].astype(str)
                                      +"_"+ GE_df['Invoice Number'].astype(str)
                                      +"_"+ GE_df['Gross Sales'].astype(str)
                                     )
    GE_df['file_name'] = blob_file_name
    #GE_df['PO_Inv_Price_Match'] = GE_df['PO_Inv_Price_Match].astype(str)
    GE_df = GE_df.drop_duplicates(subset='PO_Inv_Price_Match')
    GE_df = GE_df.drop_duplicates(subset='PO_Price_Match')
    #GE_df = GE_df.drop_duplicates(subset='Customer PO #')
    logging.info('AFTER CLEANING')
    logging.info(GE_df.dtypes)
    logging.info(len(GE_df.index))
    outcsv = GE_df.to_csv(index=False)

    cleaned_blob_file_name = "cleaned_" +blob_file_name
    block_blob_service.create_blob_from_text(out_blob_container_ge_name, cleaned_blob_file_name, outcsv)
    return "Success"
