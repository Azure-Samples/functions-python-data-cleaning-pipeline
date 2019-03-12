#%%
import logging
import requests
import json
import os
import pandas as pd
from azure.storage.blob import ContentSettings
from azure.storage.blob import BlockBlobService
from io import StringIO
from adal import AuthenticationContext
from . import fetch_blob as fetching_service
#ce4d996051a1005da9245562212cb070efdba9d2

#python3.6 -m venv funcenv... this creates the funcenv
# source funcenv/bin/activate... this activates virtual environment created above
# func host start after each change
# pip install -r requirements.txt
#ce4d996051a1005da9245562212cb070efdba9d2

#python3.6 -m venv funcenv... this creates the funcenv
# source funcenv/bin/activate... this activates virtual environment created above
# func host start after each change
# pip install -r requirements.txt
#%%
blob_account_name = os.getenv("BlobAccountName")
blob_account_key = os.getenv("BlobAccountKey")
block_blob_service = BlockBlobService(account_name=blob_account_name,
                                      account_key=blob_account_key)
out_blob_final = os.getenv("OutBlobFinal")
#%%
AUTHORITY = 'https://login.microsoftonline.com/gemtudev.onmicrosoft.com'

# Click on this link to get the Swagger API reference
base_url = 'https://gemtu-ws5arp-api.azurewebsites.net'
WORKBENCH_API_URL = 'https://gemtu-ws5arp-api.azurewebsites.net'
#base_url = 'https://gemtu-ws5arp-api.azurewebsites.net'

# This is the application ID of the blockchain workbench web API
# Login to the directory of the tenant -> App registrations -> 'Azure Blockchain Workbench *****-***** ->
# copy the Application ID
RESOURCE = 'a33cc4fb-e3f2-4c23-a005-b46819f58f07'

#Service principal app id & secret/key:
CLIENT_APP_Id = 'c8c2dab5-db8b-4ae2-8210-45b7a335708e'
CLIENT_SECRET = 'Rh95dZrJobHe3fB/GyhxhPyIRtW8DKmThmFl+CfmtI4='
#%%
auth_context = AuthenticationContext(AUTHORITY)
#%%
def clean(req_body):
    dfCreate = fetch_blobs(out_blob_final)
    #create_contract(14, 14, 1, testPayload3b)
    populate_workbench(dfCreate)
    return 'Success'
#%%
# Read/process CSV into pandas df
def fetch_blobs(out_blob_final):
    # Create container & blob dictionary with helper function
    blob_dict = fetching_service.blob_to_dict(out_blob_final)
    # create DF
    filter_string = "final"
    df = fetching_service.blob_dict_to_df(blob_dict, filter_string)
    logging.info(df.head())
    return df
#%%
def populate_workbench(dfCreate):
    #logging.warning(dfCreate.head())    
    for index, row in dfCreate.iterrows():
        try:
            logging.warning(dfCreate.iloc[index])
            payload = make_create_payload(dfCreate,index)
            outjson = payload
            blob_file_name = "df_to_json.json"
            block_blob_service.create_blob_from_text(out_blob_final, blob_file_name, outjson)
            #logging.warning(payload)
            resp = create_contract(workflowId,contractCodeId,connectionId,payload)
            createdContracts.append(resp.text)
        except:
            print('contract creation failed')
            continue
#%%
def make_create_payload(df,index):
    # This function generates the payload json fed from the pandas df
    logging.warning(df)
    #need to update this value
    workflowFunctionId = 93
    try:
        logging.warning('Creating payload...\n')
        payload = {
            "workflowFunctionId": workflowFunctionId,
            "workflowActionParameters": [
                {
                    "name": "po",
                    "value": df['PO'][index]
                }, {
                    "name": "itemno",
                    "value": df['ItemNo'][index]
                }, {
                    "name": "invno",
                    "value": df['InvNo'][index]
                }, {
                    "name": "signedinvval",
                    "value": df['SignedInvVal'][index]
                }, {
                    "name": "invdate",
                    "value": df['InvDate'][index]
                }, {
                    "name": "poformat",
                    "value": df['PO_Format'][index]
                }, {
                    "name": "popricematch",
                    "value": df['PO_Price_Match'][index]
                }, {
                    "name": "poinvpricematch",
                    "value": df['PO_Inv_Price_Match'][index]
                }, {
                    "name": "finalresult",
                    "value": df['final_result'][index]
                }
            ]
        }
        payload = json.dumps(payload)
        logging.warning('payload')
        logging.warning(payload)
        return payload
    except:
        logging.warning('error in payload')

#%%
#testPayload3b = json.dumps(testPayload3)
createdContracts = []
logging.warning('Creating contracts...\n')
def create_contract(workflowId, contractCodeId, connectionId, payload):
    if __name__ == '__main__':
        try:
            # Acquiring the token
            token = auth_context.acquire_token_with_client_credentials(
            RESOURCE, CLIENT_APP_Id, CLIENT_SECRET)
            #pprint(str(token))

            url = WORKBENCH_API_URL + '/api/v2/contracts'

            headers = {'Authorization': 'Bearer ' +
                   token['accessToken'], 'Content-Type': 'application/json'}

            params = {'workflowId': workflowId, 'contractCodeId': contractCodeId, 'connectionId': connectionId}

        # Making call to Workbench
            response = requests.post(url=url,data=payload,headers=headers,params=params)

            print('Status code: ' + str(response.status_code), '\n')
            print('Created contractId: ' + str(response.text), '\n', '\n')
            return response
        except Exception as error:
            print(error)
            return error




