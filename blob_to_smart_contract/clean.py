#%%
import logging
import requests
import json
import numpy as np
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
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
blob_service_client = BlobServiceClient(
    account_url=f"https://{blob_account_name}.blob.core.windows.net",
    credential={"account_name": f"{blob_account_name}", "account_key":f"{blob_account_key}"}
    )
out_blob_final = os.getenv("OutBlobFinal")
container_client = blob_service_client.get_container_client(container=out_blob_final)
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
    json_array= populate_workbench(dfCreate)
    result = create_json_blob(json_array)
    return 'Success'
#%%
# Read/process CSV into pandas df
def fetch_blobs(out_blob_final):
    # Create container & blob dictionary with helper function
    blob_dict = fetching_service.blob_to_dict(out_blob_final)
    # create DF
    filter_string = "final"
    df = fetching_service.blob_dict_to_df(blob_dict, filter_string)
    logging.info(df.dtypes)
    return df
#%%
def populate_workbench(dfCreate):
    json_array = []
    #logging.warning(dfCreate.head())    
    for index, row in dfCreate.iterrows():
        try:
            #logging.warning(dfCreate.iloc[index])
            payload = make_create_payload(dfCreate,index)
            json_array+=[payload]
            #logging.warning(payload)
            #resp = create_contract(workflowId,contractCodeId,connectionId,payload)
            #createdContracts.append(resp.text)
        except:
            print('contract creation failed')
            continue
    #logging.warning(payload)
    return json_array
#%%
def make_create_payload(df,index):
    # This function generates the payload json fed from the pandas df
    #logging.warning(df)
    #need to update this value
    workflowFunctionId = 93
    try:
        #logging.warning('Creating payload...\n')
        payload = {
            "workflowFunctionId": workflowFunctionId,
            "workflowActionParameters": [
                {
                    "name": "po",
                    "value": df['po'][index]
                }, {
                    "name": "itemno",
                    "value": df['itemno'][index]
                }, {
                    "name": "invno",
                    "value": df['invno'][index]
                }, {
                    "name": "signedinvval",
                    "value": df['signedinval'][index]
                }, {
                    "name": "invdate",
                    "value": df['invdate'][index]
                }, {
                    "name": "poformat",
                    "value": df['poformat'][index]
                }, {
                    "name": "popricematch",
                    "value": df['popricematch'][index]
                }, {
                    "name": "poinvpricematch",
                    "value": df['poinvpricematch'][index]
                }, {
                    "name": "initstate",
                    "value": df['initstate'][index]
                }, {
                    "name": "finalpo",
                    "value": df['finalpo'][index]
                }, {
                    "name": "finalresult",
                    "value": df['finalresult'][index]
                }
            ]
        }
        #payload = json.dumps(payload)
        #logging.warning('payload')
        #logging.warning(payload)
        return payload
    except:
        logging.warning('error in payload')

def create_json_blob(json_array):
    #outjson = json_array
    #myarray = np.asarray(json_array).tolist()
    myarray = pd.Series(json_array).to_json(orient='values')
    blob_file_name = "df_to_json.json"
    container_client.upload_blob(name=blob_file_name, data=myarray)
    return 'Success'
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




