from utils.class_utils import DataHandlerClass
from utils.mongo_utils import bulk_mongo_inserts,bulk_first_mongo_inserts
from utils.api_utils import return_async_get
from fp_types import REQUEST_ERROR,BULK_DATA_FAILED,NO_DATA,NO_META_DATA,\
                     BULK_DATA_WRITE,QUANDL_META_DATA,QUANDL_STAT_DATA
import asyncio
import aiohttp
import requests
import json 
import os 


class QuandlDataHandler(DataHandlerClass):

    def __init__(self,db_name='testdb',*args,**kwargs):
        super().__init__(db_name)
        self.quandl_secret_key = os.environ.get("QUANDL_API_KEY")
        self.quandl_meta = self.db.quandl_meta
        self.quandl_stats = self.db.quandl_stats


    def insert_list(self,code_list,insert_type):
        jump = 5 
        total_length = len(code_list)
        for i in range(0,total_length,jump):
            end = i + jump
            temp_list = code_list[i:end]
            if insert_type == "meta":
                self.insert_metadata(temp_list)
            else:
                self.insert_stats(temp_list)


    def insert_metadata(self,code_list):
        self.call_async_func('get_quandle_meta',code_list,callback_func="quandl_meta_callback")
    
    def get_metadata_list(self,code):
        temp_code_list = code.split('/')
        if len(temp_code_list)<2:
            self.log.error(NO_DATA,extra= result_json)
            return False 
        dataset_code = temp_code_list[1]
        database_code = temp_code_list[0]
        meta_query = {
            "dataset_code":dataset_code,
            "database_code":database_code
        }
        meta_data_list = list(self.quandl_meta.find(meta_query))
        return meta_data_list

    def insert_stats(self,code_list):
        new_code_list = []
        for code in code_list:
            metadata = self.get_metadata_list(code)
            if not metadata:
                last_start = None 
            else:
                meta_id = metadata[0].get('id')
                last_stat = self.quandl_stats.find_one({"id":meta_id},{"Date":1},sort=[("DATE",-1)])
                last_start = last_stat.get("Date") if last_stat else None 
            data_map = {}
            data_map['code'] = code
            data_map['start_date'] = last_start
            new_code_list.append(data_map)
        self.call_async_func(
            'get_quandle_stats',
            new_code_list,
            callback_func="quandl_insert_callback",
            callback_args=True
        )

    def insert_first_stats(self,code_list):
        self.call_async_func(
            'get_quandle_stats',
            code_list,
            callback_func="quandl_first_insert_callback",
            callback_args=True
        )


    async def get_quandle_meta(self,code):
        url = f'https://www.quandl.com/api/v3/datasets/{code}/metadata.json?api_key={self.quandl_secret_key}'
        return await return_async_get(url)



    async def get_quandle_stats(self,code,start_date=None):
        if not start_date:
            url = f'https://www.quandl.com/api/v3/datasets/{code}/data.json?api_key={self.quandl_secret_key}'
            return await return_async_get(url)
        else:
            url = f'https://www.quandl.com/api/v3/datasets/{code}/data.json?api_key={self.quandl_secret_key}&start_date={start_date}'
            return await return_async_get(url)



    def quandl_meta_callback(self,future):
        metadata_result = future.result()
        result_json = metadata_result.get('data')
        if metadata_result.get('result') == "fail":
            self.log.error(REQUEST_ERROR,extra= result_json)
            return False 
        if not result_json.get('dataset'):
            self.log.error(NO_DATA,extra= result_json)
            return False 
        result = result_json['dataset']
        result_list = [result]
        results =bulk_mongo_inserts(result_list,self.quandl_meta,['database_code','dataset_code'],self.log,QUANDL_META_DATA)
        return results


    def prepare_stats_insert_data(self,metadata_result,code):
        result_json = metadata_result.get('data')
        if metadata_result['result'] == "fail":
            self.log.error(REQUEST_ERROR,extra= result_json)
            return False 
        if not result_json.get('dataset_data') or not result_json['dataset_data'].get('data'):
            self.log.error(NO_DATA,extra= result_json)
            return False 
        return_list = []
        result_list = result_json['dataset_data']['data']
        columns_list = result_json['dataset_data']['column_names']
        temp_code_list = code.split('/')
        if len(temp_code_list)<2 or not result_list:
            self.log.error(NO_DATA,extra= result_json)
            return False 
        dataset_code = temp_code_list[1]
        database_code = temp_code_list[0]
        meta_query = {
            "dataset_code":dataset_code,
            "database_code":database_code
        }
        meta_data_list = list(self.quandl_meta.find(meta_query))
        if not meta_data_list:
            self.log.error(NO_DATA,extra= meta_query)
            return False 
        meta_elem = meta_data_list[0]
        meta_id = meta_elem.get('id')
        for data in result_list:
            data_map = {}
            for i in range(len(columns_list)):
                columns_list[i] = columns_list[i].replace(".",'')
                data_map[columns_list[i]] = data[i] 
                data_map['id'] = meta_id
                data_map['database_code'] = database_code
                data_map['dataset_code'] = dataset_code
            return_list.append(data_map)
        return return_list
    
    def quandl_insert_callback(self,data_map,future):
        code = data_map.get('code')
        start_date = data_map.get('start_date')
        metadata_result = future.result()
        return_list = self.prepare_stats_insert_data(metadata_result,code)
        if not return_list:
            return False 
        if start_date is None:
            results =bulk_first_mongo_inserts(return_list,self.quandl_stats,self.log,QUANDL_STAT_DATA)
                
        else:
            results =bulk_mongo_upserts(return_list,self.quandl_stats,['id','Date'],self.log,QUANDL_STAT_DATA)

    def quandl_first_insert_callback(self,code,future):
        metadata_result = future.result()
        return_list = self.prepare_stats_insert_data(metadata_result,code)
        results =bulk_first_mongo_inserts(return_list,self.quandl_stats,self.log,QUANDL_STAT_DATA)
        