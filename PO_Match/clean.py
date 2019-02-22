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
#

blob_account_name = os.getenv("BlobAccountName")
blob_account_key = os.getenv("BlobAccountKey")
block_blob_service = BlockBlobService(account_name=blob_account_name,
                                      account_key=blob_account_key)
out_blob_container_name = os.getenv("OutBlobContainerName")
out_blob_container_ge_name = os.getenv("OutBlobContainerGEName")
out_blob_pomatch = os.getenv("OutBlobPoMatchFormatted")
# Clean blob flow from event grid events
# This function will call all the other functions in clean.py
def clean(req_body):
    formatted_df = categorize_PO_format_in_customer_file(out_blob_container_name,out_blob_container_ge_name)
    #formatted_df = format_total_blob(po_match_df)
    #formatted_ge_df = format_ge_blob(ge_df)
    result = format_cleaned_customer_file(formatted_df)
    return result

# Reconcile GE and Customer Files
# Reconcile GE and Customer Files
def categorize_PO_format_in_customer_file(out_blob_container_name,out_blob_container_ge_name):
    # This function will add the PO Format Category to the Customer File
    # Get the blob names in the MTU blob and read in the latest to a df
    generator = block_blob_service.list_blobs(out_blob_container_name)
    MTU_blob_file = []
    for blob in generator:
        print("\t Blob name: " + blob.name)  
        MTU_blob_file.append(blob.name)
    logging.warning('MTU Reconciliation')
    MTU_blob_latest_file = MTU_blob_file[-1]
    # Need to convert the list to a string for it to work with the python Azure SDK methods below
    ''.join([str(i) for i in MTU_blob_latest_file])
    logging.info(MTU_blob_latest_file)
    
    # Convert Customer Blob File to DF
    #https://stackoverflow.com/questions/33091830/how-best-to-convert-from-azure-blob-csv-format-to-pandas-dataframe-while-running
    MTU_blobstring = block_blob_service.get_blob_to_text(out_blob_container_name,MTU_blob_latest_file).content
    MTU_df = pd.read_csv(StringIO(MTU_blobstring),dtype={'PurchDoc': object,'Invoice No.': object,'Item': object,
         'GE_PO_#': object, 'MTU_PO_#': object})
    logging.info(MTU_df.head(5))
    logging.info(MTU_df.dtypes)   
    logging.info(len(MTU_df.index))
    # convert column to be matched to GE_df to string
    #MTU_df['GE_PO_#'] = MTU_df['GE_PO_#'].astype(str)

    # Kick off GE Process... same as above
    logging.warning('GE Reconciliation')
    generator_ge = block_blob_service.list_blobs(out_blob_container_ge_name)
    GE_blob_file = []
    for blob in generator_ge:
        print("\t Blob name: " + blob.name)  
        GE_blob_file.append(blob.name)
    GE_blob_latest_file = GE_blob_file[-1]
    # Need to convert the list to a string for it to work with the python Azure SDK methods below
    ''.join([str(i) for i in GE_blob_latest_file])
    logging.info(GE_blob_latest_file)
    GE_blobstring = block_blob_service.get_blob_to_text(out_blob_container_ge_name,GE_blob_latest_file).content
    GE_df = pd.read_csv(StringIO(GE_blobstring))
    # Need to drop duplicates or you will get more rows than the initial customer DF has.
    GE_df = GE_df.drop_duplicates(subset='Customer PO #')
 
    logging.info(GE_df.head(5))
    logging.info(GE_df.dtypes)
    logging.info(len(GE_df.index))
       
    ## We need to check the GE file for POs in the GE Format and the MTU Format
    ## First, let's left match on MTU df to GE PO Format
    PO_Match_1_df = MTU_df.merge(GE_df.drop_duplicates(), on='GE_PO_#', how='left', indicator='PO_Match_GE')
    logging.info(PO_Match_1_df.dtypes)
    # Both means we have a match between each df.  Let's rename 'both' to 'GE_PO_Format'
    PO_Match_1_df['PO_Match_GE'] = PO_Match_1_df['PO_Match_GE'].str.replace('both','GE_PO_Format')
    #outcsv_po = PO_Match_1_df.to_csv(index=False)
    #blob_file_name = "po_match_on_GE_PO_Format"
    #block_blob_service.create_blob_from_text(out_blob_pomatch, blob_file_name , outcsv_po)
    
    ## Next, lets left match check to see if any MTU POs match the GE file
    ## We will keep our progress from rows that we matched to the GE format in the PO_Match_1_df above.
    # We just matched the GE PO in the previous step, so we can drop that column now.
    # The GE_PO_# is just an extra column that we copied the PO column from the GE df in order to do a left join.
    GE_df.drop(['GE_PO_#'], axis=1, inplace = True)
    # Create MTU PO Column in GE df for merge
    GE_df['MTU_PO_#'] = GE_df['Customer PO #']
    PO_Match_2_df = PO_Match_1_df.merge(GE_df.drop_duplicates(), on='MTU_PO_#',
        how='left', indicator='PO_Match_MTU')
    # Rename to ensure we can see GE vs MTU PO Match
    PO_Match_2_df['PO_Match_MTU'] = PO_Match_2_df['PO_Match_MTU'].str.replace('both','MTU_PO_Format')
    #outcsv_po = PO_Match_2_df.to_csv(index=False)
    #blob_file_name = "po_match_on_MTU_PO_Format"
    #block_blob_service.create_blob_from_text(out_blob_pomatch, blob_file_name , outcsv_po)
    #cleaned_df = format_total_blob(PO_Match_2_df)
    drop = ['Gross Sales_y','ESN_y','Invoice Number_y','Order #_y','Customer PO #_y',
        'Gross Sales_x','ESN_x','Invoice Number_x','Order #_x','Customer PO #_x', 
        'PO_Inv_Price_Match_x','PO_Price_Match_x','PO_Inv_Price_Match_y','PO_Price_Match_y']
    PO_Match_2_df.drop(drop, axis=1, inplace = True)
    logging.info(PO_Match_2_df.dtypes)
    #format_ge_blob(GE_df)

    #outcsv = PO_Match_2_df.to_csv(index=False)
    #blob_file_name = "cleaned_PO_Match.csv"
    #block_blob_service.create_blob_from_text(out_blob_pomatch, blob_file_name, outcsv)
    return PO_Match_2_df

def format_cleaned_customer_file(df):
    #logging.info(content)
    cleaned_df = df
    #Lets add a column that shows the final PO category that reconciled
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
    # 
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
    outcsv = cleaned_df.to_csv(index=False)
    blob_file_name = "Customer_Cleaned_PO_Match.csv"
    block_blob_service.create_blob_from_text(out_blob_pomatch, blob_file_name, outcsv)
    return "Success"
    