import logging
import os
import pandas as pd
from azure.storage.blob import ContentSettings
from azure.storage.blob import BlockBlobService
from io import StringIO
from . import blob_fetch as data_service


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
out_blob_container_name = os.getenv("OutBlobPoMatchFormatted")
out_blob_final = os.getenv("OutBlobFinal")

all_data = []
# Clean blob flow from event grid events
def clean(req_body):
    #blob_obj,filename = extract_blob_props(req_body[0]['data']['url']  )
    #final_match(out_blob_container_name,out_blob_container_ge_name)
    df = final_match(out_blob_container_name,out_blob_container_ge_name)
    GE_df, MTU_df = final_df_date_pairing(df) 
    result = final_reconciliation(GE_df, MTU_df)
    return 'Success'

# Extract blob container name and blob file
def extract_blob_props(container, file):
    readblob = block_blob_service.get_blob_to_text(container, 
                                                   file).content
    logging.warning('Extract_Blob_Props')
    file_name = file.rsplit('.',1)[-2]
    #df_name = 'df_' + file_name
    df = pd.read_csv(StringIO(readblob))
    all_data.append(df)
    LLP_df = pd.concat(all_data)
    logging.warning(len(LLP_df.index))
    return LLP_df

def final_match(out_blob_container_name,out_blob_container_ge_name):
    blob_list = []
    blob_list.append(out_blob_container_name)
    blob_list.append(out_blob_container_ge_name)
    ''.join([str(i) for i in blob_list])
    #logging.info(blob_list)
    #i = 0
    file_names = []
    i = 0
    temp = []
    for container in blob_list:
        logging.warning('FOR LOOP')
        generator = block_blob_service.list_blobs(container)
        logging.info(container)
        for file in generator:
            logging.warning('Hey Bob')
            file_names.append(file.name)
            logging.info(file_names[i]) 
            #https://stackoverflow.com/questions/6181935/how-do-you-create-different-variable-names-while-in-a-loop
            logging.warning('Bye Bob')
            df = extract_blob_props(container,file_names[i])
            i = i+1
            #https://bytes.com/topic/python/answers/902052-create-variable-each-iteration-loop
    return df

def final_df_date_pairing(df):
    #https://stackoverflow.com/questions/28311655/ignoring-nans-with-str-contains
    #GE_df = df[df['file_name'].str.contains('GE')]
    logging.warning(df.dtypes)
    logging.warning('******* Date Pairing *************')
    GE_df = df.loc[df.file_name.str.contains("GE", na=False)]
    logging.warning('******* Date Pairing *************')
    logging.warning(len(GE_df.index))
    MTU_df = df.loc[df.file_name_x.str.contains("MTU", na=False)]
    logging.warning('******* Date Pairing 2*************')
    logging.warning(len(MTU_df.index))
    
    outcsv = GE_df.to_csv(index=False)
    blob_file_name = "GE_Final_File.csv"
    block_blob_service.create_blob_from_text(out_blob_final, blob_file_name, outcsv)
    return MTU_df, GE_df

def final_reconciliation(MTU_df, GE_df):    
    # GE_df is much larger than customer df, so we need to drop any duplicates of the columns we are merging on.
    GE_df = GE_df.drop_duplicates(subset='PO_Inv_Price_Match')
    GE_df = GE_df.drop_duplicates(subset='PO_Price_Match')
    GE_level_1 = GE_df.loc[:, ['PO_Inv_Price_Match','ESN']]
    outcsv = GE_level_1.to_csv(index=False)
    blob_file_name = "GE_Final_File Level 1.csv"
    block_blob_service.create_blob_from_text(out_blob_final, blob_file_name, outcsv)
    GE_level_2 = GE_df.loc[:, ['PO_Price_Match','ESN']]
    Level_1_df = MTU_df.merge(GE_level_1.drop_duplicates(), on=['PO_Inv_Price_Match'], 
                   how='left', indicator='level_1')
    Level_1_df['level_1'] = Level_1_df['level_1'].str.replace('both','level_1_match')
    Level_2_df = Level_1_df.merge(GE_level_2.drop_duplicates(), on=['PO_Price_Match'], 
                   how='left', indicator='level_2')
    Level_2_df['level_2'] = Level_2_df['level_2'].str.replace('both','level_2_match')

    # Create a final column to show the match status
    final_result = []
    for index, row in Level_2_df.iterrows():
        if row['level_1'] == 'level_1_match':
            final_result.append('PO_Inv_Price_Match')
        elif row['level_1'] == 'left_only':
            if row['level_2'] == 'level_2_match':
                final_result.append('PO_Price_Match')
            else:
                final_result.append('no_match')
        else:
            final_result.append('mismatch')
    Level_2_df['final_result'] = final_result
    logging.warning('******* Final_DF*************')
    logging.warning(len(Level_2_df.index))
    #https://chrisalbon.com/python/data_wrangling/pandas_create_column_with_loop/
        #drop = ['PO_Match_MTU','PO_Match_GE']
    #cleaned_df.drop(drop, axis=1, inplace = True)
    outcsv = Level_2_df.to_csv(index=False)
    blob_file_name = "Final_Matched_File.csv"
    block_blob_service.create_blob_from_text(out_blob_final, blob_file_name, outcsv)
    return "Success"
    #logging.warning('Second Try')
    #var1 = block_blob_service.list_blobs(out_blob_container_name)
    #for x in var1:
    #    logging.info("\t Blob name: " + x.name)  
        #GE_blob_file.append(blob.name)
    #blob_list.append(block_blob_service.list_blobs(out_blob_container_name))
    #blob_list.append(block_blob_service.list_blobs(out_blob_container_ge_name))

    #logging.info(blob_list)
    #i = 0
    #generator= []
    #return 'Success'
    