---
page_type: sample
description: "This sample demonstrates a data cleaning pipeline with Azure Functions written in Python."
languages:
- python
products:
- azure-functions
- azure-storage
---

# Data Cleaning Pipeline

This sample demonstrates a data cleaning pipeline with Azure Functions written in Python triggered off a HTTP event from Event Grid to perform some pandas cleaning and reconciliation of CSV files.
Using this sample we demonstrate a real use case where this is used to perform cleaning tasks.

## Getting Started

### Deploy to Azure

#### Prerequisites

- Install Python 3.6+
- Install [Functions Core Tools](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local#v2)
- Install Docker
- Note: If run on Windows, use Ubuntu WSL to run deploy script

#### Steps

- Deploy through Azure CLI
    - Open AZ CLI and run `az group create -l [region] -n [resourceGroupName]` to create a resource group in your Azure subscription (i.e. [region] could be westus2, eastus, etc.)
    - Run `az group deployment create --name [deploymentName] --resource-group [resourceGroupName] --template-file azuredeploy.json`

- Deploy Function App
  - [Create/Activate virtual environment](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-function-python#create-and-activate-a-virtual-environment)
  - Run `func azure functionapp publish [functionAppName] --build-native-deps` 

### Test

- Upload s1.csv file into c1raw container
- Watch event grid trigger the CleanTrigger1 function and produce a "cleaned_s1_raw.csv"
- Repeat the same for s2.csv into c2raw container
- Now send the following HTTP request to the Reconcile function to merge

```
{
	"file_1_url" : "https://{storagename}.blob.core.windows.net/c1raw/cleaned_s1_raw.csv",
	"file_2_url" : "https://{storagename}.blob.core.windows.net/c2raw/cleaned_s2_raw.csv",
	"batchId" : "1122"
}

```
- Watch it produce final.csv file 
- Can use a logic app to call the reconcile method with batch id's

## References

- [Create your first Python Function](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-first-function-python)
