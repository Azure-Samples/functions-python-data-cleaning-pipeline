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
out_blob_container_name = os.getenv("OutBlobContainerName")
out_blob_container_ge_name = os.getenv("OutBlobContainerGEName")

# Clean blob flow from event grid events
def clean(req_body):
    blob_obj,filename = extract_blob_props(req_body[0]['data']['url']  )
    result = clean_blob(blob_obj.content,filename)
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

def mtu_clean_blob(content, blob_file_name):
    MTU_df = pd.read_csv(StringIO(content))
    logging.warning('MTU Cleaning')
    logging.info('BEFORE CLEANING')
    logging.info(MTU_df.head(5)) 
    MTU_df['PurchDoc'] = MTU_df['PurchDoc'].astype(str)
    MTU_df['Item'] = MTU_df['Item'].astype(str)
    MTU_df['PurchDoc_8'] = MTU_df['PurchDoc'].str[-8:]
    MTU_df['Item_3'] = MTU_df['Item'].str[-3:]
    MTU_df['GE_PO_#'] = MTU_df['PurchDoc_8'] + MTU_df['Item_3']
    MTU_df['MTU_PO_#'] = MTU_df['PurchDoc']
    logging.info('AFTER CLEANING')
    logging.info(MTU_df.dtypes) 
    outcsv = MTU_df.to_csv(index=False)
    cleaned_blob_file_name = "cleaned_" +blob_file_name
    # TODO Clean blob data logic here
    block_blob_service.create_blob_from_text(out_blob_container_name, cleaned_blob_file_name , outcsv)
    
    return cleaned_blob_file_name
    #return MTU_df

def ge_clean_blob(content, blob_file_name):
    #fires on http trigger 2
    GE_df = pd.read_csv(StringIO(content))
    logging.warning('GE Cleaning')
    logging.info('BEFORE CLEANING')
    logging.info(GE_df.head(5)) 
    GE_df['GE_PO_#'] = GE_df['Customer PO #']
    GE__df = GE_df.drop_duplicates(subset='Customer PO #')
    #logging.info('AFTER CLEANING')
    logging.info(GE_df.dtypes) 
    outcsv = GE_df.to_csv(index=False)

    cleaned_blob_file_name = "cleaned_" +blob_file_name
    block_blob_service.create_blob_from_text(out_blob_container_ge_name, cleaned_blob_file_name, outcsv)
    return cleaned_blob_file_name

# process blob, clean, preprocess etc
def clean_blob(content, blob_file_name):
    # this function will now reconcile and combine two cleaned dataframes
    MTU_file_name = mtu_clean_blob(content, blob_file_name)
    GE_status = ge_clean_blob(content, blob_file_name)
    #show file being read in
    logging.warning('MTU Reconciliation Blob Testing')
    #https://realpython.com/python-logging/
    #logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    logging.info(MTU_file_name)
    #logging.info(blob_df.head(5))
    #https://stackoverflow.com/questions/33091830/how-best-to-convert-from-azure-blob-csv-format-to-pandas-dataframe-while-running
    MTU_blobstring = block_blob_service.get_blob_to_text(out_blob_container_name,MTU_file_name).content
    MTU_df = pd.read_csv(StringIO( MTU_blobstring))
    logging.info(MTU_df.head(5))
    logging.info(MTU_df.dtypes)   
    #logging.warning('GE Clean Blob Testing')
    #logging.info(GE_df.head(5))
    #df = pd.read_csv(StringIO(content))
    #logging.info('********blob content***********')  
    #logging.info(df.head(5))   
    #print (df.info)
    #df1 = df.iloc[:,0:4] 
    #outcsv = df1.to_csv(index=False)
    #block_blob_service = BlockBlobService(account_name, account_key)
    # https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.baseblobservice.baseblobservice?view=azure-python

    # List the blobs in the container
    #logging.warning('List blobs in the container')
    #generator = block_blob_service.list_blobs(out_blob_container_name)
    #blob_file = []
    #for blob in generator:
    #    print("\t Blob name: " + blob.name)  
    #    blob_file.append(blob.name)
    #    logging.info(blob_file)
         #list.append(['e','f'])
        #blob_df=  blob_file + "_df"
        #blob_df = pd.read_csv(StringIO(content))
        #logging.info(blob_df.head(5))
        #https://stackoverflow.com/questions/33091830/how-best-to-convert-from-azure-blob-csv-format-to-pandas-dataframe-while-running
        #blobstring = block_blob_service.get_blob_to_text(out_blob_container_name)blob_file).content
    #logging.info(len(blob_file))
    #logging.info(blob_file[0])
    # List comprehension for creating seperate df
    #ge_list = [x for x in blob_file if "GE" in x]
    #logging.info(ge_list)
    #blobstring = block_blob_service.get_blob_to_text(out_blob_container_name,ge_list).content
    #ge_df = pd.read_csv(StringIO(blobstring))
    #mtu_list = [x for x in blob_file if "MTU" in x]
    #logging.info(mtu_list)
    # TODO Clean blob data logic here
    #block_blob_service.create_blob_from_text(out_blob_container_name, "clean_" +blob_file_name , outcsv)
    return "Success"