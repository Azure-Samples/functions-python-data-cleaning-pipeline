import logging
import json
import azure.functions as func
from . import clean as cleaning_service

def main(req: func.HttpRequest) -> func.HttpResponse:
    # This will output to postman
    logging.info('Python HTTP trigger function processed a request.')
    try:
        req_body = req.get_json()
        f1_url = req_body.get('file_1_url')
        f2_url = req_body.get('file_2_url')
        batch_id = req_body.get('batchId')
    except:
        return func.HttpResponse("Bad Request", status_code=400)

    result = cleaning_service.clean(f1_url,f2_url,batch_id)
    return func.HttpResponse(result,status_code=200)