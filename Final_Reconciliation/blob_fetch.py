def file_fetch(out_blob_container_name,out_blob_container_ge_name):
    blob_list = []
    blob_list.append(out_blob_container_name)
    blob_list.append(out_blob_container_ge_name)
    for blob in blob_list:
        block_blob_service.list_blobs(blob)
        print("\t Blob name: " + blob.name)  
    return 'Success'
    generator = block_blob_service.list_blobs(out_blob_container_name)
    MTU_blob_file = []
    for blob in generator:
        
        MTU_blob_file.append(blob.name)
    logging.warning('MTU Reconciliation')
    MTU_blob_latest_file = MTU_blob_file[-1]
    # Need to convert the list to a string for it to work with the python Azure SDK methods below
    ''.join([str(i) for i in MTU_blob_latest_file])
    logging.info(MTU_blob_latest_file)
