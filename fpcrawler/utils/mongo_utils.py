
from fp_types.errors import BULK_DATA_FAILED,NO_DATA
from fp_types.apitypes import BULK_DATA_WRITE


def bulk_mongo_upserts(
        data_list,insert_col,search_keys,logger,data_name,
        log_add_data=None
    ):
    update_list = []
    for row in data_list:
        search_dict = { key:row[key] for key in search_keys}
        update_list.append(
            UpdateOne(
                search_dict,
                {"$set":row},
                upsert=True))                
    extra_data = {}
    extra_data['dataName'] = data_name
    if log_add_data:
        for key in log_add_data:
            if key != "_id":
                extra_data[key] = log_add_data[key]

    if not update_list:
        extra_data['errorMessage'] = NO_DATA
        logger.error(BULK_DATA_FAILED,extra=extra_data) 
        return False 

    try:
        result = insert_col.bulk_write(update_list)
        logger.info(BULK_DATA_WRITE,extra=extra_data) 
        return True 
    except BulkWriteError as bwe:
        extra_data['errorMessage'] = bwe.details
        logger.error(BULK_DATA_FAILED,extra=extra_data)
        return False 
    except InvalidDocument as e: 
        extra_data['errorMessage'] = str(e)
        logger.error(BULK_DATA_FAILED,extra=extra_data)
        return False 


def bulk_mongo_inserts(
        data_list,insert_col,search_keys,logger,data_name,
        log_add_data=None
    ):
    update_list = []


    extra_data = {}
    extra_data['dataName'] = data_name
    if log_add_data:
        for key in log_add_data:
            if key != "_id":
                extra_data[key] = log_add_data[key]
    try:
        search_dict = { key:data_list[0][key] for key in search_keys}
        insert_col.delete_many(search_dict)
        insert_col.insert_many(data_list)       
        logger.info(BULK_DATA_WRITE,extra=extra_data) 
        return True 
    except Exception as e:
        extra_data['errorMessage'] = str(e)
        logger.error(BULK_DATA_FAILED,extra=extra_data)
        return False 

    
def bulk_first_mongo_inserts(data_list,insert_col,logger,data_name,log_add_data=None):
    inserted_data = {}
    inserted_data['dataName'] = data_name
    if log_add_data:
        for key in log_add_data:
            inserted_data[key] = log_add_data[key]

    if not data_list:
        inserted_data['errorMessage'] = NO_DATA
        logger.error(BULK_DATA_FAILED,extra=inserted_data)
    try:
        result = insert_col.insert_many(data_list)
        inserted_ids = [ str(oid) for oid in result.inserted_ids]
        inserted_data['insertedIds'] = inserted_ids
        logger.info(BULK_DATA_WRITE,extra=inserted_data) 
        return True 
    except BulkWriteError as bwe:
        inserted_data['errorMessage'] = bwe.details
        logger.error(BULK_DATA_FAILED,extra=inserted_data)
        return False 
    except InvalidDocument as e:
        inserted_data['errorMessage'] = str(e)
        logger.error(BULK_DATA_FAILED,extra=inserted_data)
        return False 

        

    