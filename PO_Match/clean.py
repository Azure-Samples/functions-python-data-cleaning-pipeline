import logging
import os
import pandas as pd
from azure.storage.blob import ContentSettings
from azure.storage.blob import BlockBlobService
from io import StringIO
#ce4d996051a1005da9245562212cb070efdba9d2

#python3.6 -m venv funcenv... this creates the funcenv
# source funcenv/bin/activate... this activates virtual environment created above
# func host start after each change
# pip install -r requirements.txt

blob_account_name = os.getenv("BlobAccountName")
blob_account_key = os.getenv("BlobAccountKey")
block_blob_service = BlockBlobService(account_name=blob_account_name,
                                      account_key=blob_account_key)
out_blob_container_name = os.getenv("OutBlobContainerName")
out_blob_container_ge_name = os.getenv("OutBlobContainerGEName")
out_blob_pomatch = os.getenv("OutBlobPoMatchFormatted")
out_blob_final = os.getenv("OutBlobFinal")
# Clean blob flow from event grid events
# This function will call all the other functions in clean.py
def clean(req_body):
    MTU_df, GE_df = fetch_blobs(out_blob_container_name,out_blob_container_ge_name)
    cleaned_df = determine_PO_format(MTU_df, GE_df)
    result = final_reconciliation(cleaned_df, GE_df)
    return 'Success'

def extract_blob_props(container,filename_list):
    latest_file = filename_list[-1]
    # Need to convert the list to a string for it to work with the python Azure SDK methods below
    ''.join([str(i) for i in latest_file])
    logging.warning(latest_file)
    blobstring = block_blob_service.get_blob_to_text(container, latest_file).content
    df = pd.read_csv(StringIO(blobstring),dtype=str)
    return df

def fetch_blobs(out_blob_container_name,out_blob_container_ge_name):
    # This function will get the blobs and read them into dataframes
    mtu_generator = block_blob_service.list_blobs(out_blob_container_name)
    ge_generator = block_blob_service.list_blobs(out_blob_container_ge_name)
    # this is a generator object and now we need to get the file name
    logging.warning(mtu_generator)

    # Create Customer DF
    MTU_blob_file = []
    for blob in mtu_generator:
        MTU_blob_file.append(blob.name)
    MTU_df = extract_blob_props(out_blob_container_name,MTU_blob_file)
    logging.info(MTU_df.head(5))
    logging.info(MTU_df.dtypes)   
    logging.warning(len(MTU_df.index))

    ### Create GE DF
    ge_blob_file = []
    for ge_file in ge_generator:
        ge_blob_file.append(ge_file.name)
        logging.warning(ge_blob_file)
        GE_df = extract_blob_props(out_blob_container_ge_name,ge_blob_file)
    logging.info(GE_df.head(5))
    logging.info(GE_df.dtypes)   
    logging.warning(len(GE_df.index))
    return MTU_df, GE_df

def determine_PO_format(MTU_df, GE_df):
    ## We need to check the GE file for POs in the GE Format and the MTU Format
    ## First, let's left match on MTU df to GE PO Format
    GE_df = GE_df.drop_duplicates(subset='Customer PO #')
    logging.warning('**************MTU_DF_LEN')      
    logging.warning(len(MTU_df.index))
    PO_Match_1_df = MTU_df.merge(GE_df.drop_duplicates(), on='GE_PO_#', how='left', indicator='PO_Match_GE')
    logging.warning('**************PO_Match_1_df')      
    logging.warning(len(PO_Match_1_df.index))
    # Both means we have a match between each df.  Let's rename 'both' to 'GE_PO_Format'
    PO_Match_1_df['PO_Match_GE'] = PO_Match_1_df['PO_Match_GE'].str.replace('both','GE_PO_Format')
    ## Next, lets left match check to see if any MTU POs match the GE file
    ## We will keep our progress from rows that we matched to the GE format in the PO_Match_1_df above.
    # We just matched the GE PO in the previous step, so we can drop that column now.
    # The GE_PO_# is just an extra column that we copied the PO column from the GE df in order to do a left join.
    GE_df.drop(['GE_PO_#'], axis=1, inplace = True)
    # Create MTU PO Column in GE df for merge
    logging.info(PO_Match_1_df.dtypes)
    GE_df['MTU_PO_#'] = GE_df['Customer PO #']
    cleaned_df = PO_Match_1_df.merge(GE_df.drop_duplicates(), on='MTU_PO_#',
        how='left', indicator='PO_Match_MTU')
    logging.warning('**************cleaned_df')      
    logging.warning(len(cleaned_df.index))
    # Rename to ensure we can see GE vs MTU PO Match
    cleaned_df['PO_Match_MTU'] = cleaned_df['PO_Match_MTU'].str.replace('both','MTU_PO_Format')
    drop = ['Gross Sales_y','ESN_y','Invoice Number_y','Order #_y','Customer PO #_y',
        'Gross Sales_x','ESN_x','Invoice Number_x','Order #_x','Customer PO #_x', 
        'PO_Inv_Price_Match_x','PO_Price_Match_x','PO_Inv_Price_Match_y','PO_Price_Match_y']
    cleaned_df.drop(drop, axis=1, inplace = True)
    logging.info(cleaned_df.dtypes)
    # #Lets add a column that shows the final PO category that reconciled
    PO_result = []
    for index, row in cleaned_df.iterrows():
        if row['PO_Match_GE'] == 'GE_PO_Format':
            PO_result.append('GE_PO_Format')
        elif row['PO_Match_MTU'] == 'MTU_PO_Format':
            PO_result.append('MTU_PO_Format')
        else:
            PO_result.append('no_match')
    
        # Update cleaned_df column with list of categories created above.       
    cleaned_df['PO_Format'] = PO_result
    # Lets add a column that shows the final PO # that reconciled
    mask = cleaned_df['PO_Match_GE'] == 'GE_PO_Format'
    cleaned_df.loc[mask,'Final_PO'] = cleaned_df.loc[mask,'GE_PO_#']
    #https://stackoverflow.com/questions/35728838/pandas-get-an-if-statement-loc-to-return-the-index-for-that-row
    del mask
    mask = cleaned_df['PO_Match_MTU'] == 'MTU_PO_Format'
    cleaned_df.loc[mask,'Final_PO'] = cleaned_df.loc[mask,'MTU_PO_#']
    
    # Remember, the cleaned_df is joined on the MTU dataframe and contains the final_PO Category
    # https://pandas.pydata.org/pandas-docs/stable/cookbook.html
    # Create column for PO + Price Match
    cleaned_df['PO_Price_Match'] = (cleaned_df['Final_PO'].astype(str) +"_"+ cleaned_df['SignedInvVal'].astype(str))
    
    # https://pandas.pydata.org/pandas-docs/stable/cookbook.html

    # Create column for PO + Inv + Price Match
    cleaned_df['PO_Inv_Price_Match'] = (cleaned_df['Final_PO'].astype(str) +"_"+ cleaned_df['Invoice No.'].astype(str) 
                                  +"_"+ cleaned_df['SignedInvVal'].astype(str))         
    
    #drop = ['Gross Sales_y','ESN_y','Invoice Number_y','Order #_y','Customer PO #_y',
    #        'Gross Sales_x','ESN_x','Invoice Number_x','Order #_x','Customer PO #_x']
    #cleaned_df.drop(drop, axis=1, inplace = True)

    drop = ['PO_Match_MTU','PO_Match_GE']
    cleaned_df.drop(drop, axis=1, inplace = True)
    # return MTU df with PO match info
    return cleaned_df

def final_reconciliation(cleaned_df, GE_df):    
    # GE_df is much larger than customer df, so we need to drop any duplicates of the columns we are merging on.
    GE_df = GE_df.drop_duplicates(subset='PO_Inv_Price_Match')
    GE_df = GE_df.drop_duplicates(subset='PO_Price_Match')
    GE_level_1 = GE_df.loc[:, ['PO_Inv_Price_Match']]
    outcsv = GE_level_1.to_csv(index=False)
    blob_file_name = "GE_Final_File Level 1.csv"
    block_blob_service.create_blob_from_text(out_blob_pomatch, blob_file_name, outcsv)
    GE_level_2 = GE_df.loc[:, ['PO_Price_Match']]
    
    Level_1_df = cleaned_df.merge(GE_level_1.drop_duplicates(), on=['PO_Inv_Price_Match'], 
                   how='left', indicator='level_1')
    logging.warning('**************CLEANED_DF_LEN')      
    logging.warning(len(cleaned_df.index))
    Level_1_df['level_1'] = Level_1_df['level_1'].str.replace('both','level_1_match')
    logging.info(Level_1_df.head(5))
    logging.info(Level_1_df.dtypes)
    logging.warning(len(Level_1_df.index))
    outcsv = Level_1_df.to_csv(index=False)
    blob_file_name = "Level_1_Match.csv"
    block_blob_service.create_blob_from_text(out_blob_pomatch, blob_file_name, outcsv)

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