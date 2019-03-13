import logging
import json
import azure.functions as func
from . import clean as cleaning_service

def main(req: func.HttpRequest) -> func.HttpResponse:
    # This will output to postman
    logging.info('Python HTTP trigger function processed a request.')
    try:
        req_body = req.get_json()
        ge_file_url = req_body.get('ge_file_url')
        customer_file_url = req_body.get('customer_file_url')
        batch_id = req_body.get('batchId')
    except:
        return func.HttpResponse("Bad Request", status_code=400)
    
    #try:
    result = cleaning_service.clean(ge_file_url,customer_file_url,batch_id)
    return func.HttpResponse(result,status_code=200)
    #except:
     #   return func.HttpResponse("Failed", status_code=500)