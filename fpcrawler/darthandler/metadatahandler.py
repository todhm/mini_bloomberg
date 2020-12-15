from utils.class_utils import DataHandlerClass
from utils.mongo_utils import bulk_mongo_inserts
from utils.api_utils import return_async_get
from fp_common.fp_types import * 
import asyncio
import aiohttp
import requests
import json 
import os 


class MetaDataHandler(DataHandlerClass):

    def __init__(self,db_name='testdb',*args,**kwargs):
        super().__init__(db_name)
        self.bank_secret_key = os.environ.get("KOREAN_BANK_SECRET_KEY")
        self.bok_stat_code = self.db.bok_stat_code
        self.bok_item_code = self.db.bok_item_code


    def insert_metadata(self):
        url = "http://ecos.bok.or.kr/api/"\
            f"StatisticTableList/{self.bank_secret_key}/json/kr/1/1000"
        try:
            parent_json = json.loads(requests.get(url).text)
            parent_json = parent_json['StatisticTableList']['row']
        except Exception as e:
            request_error = str(e)
            error_data = {}
            error_data['errorMessage'] = request_error
            self.log.error(REQUEST_ERROR,extra=error_data)
            return False 

        update_list = []
        results =bulk_mongo_inserts(parent_json,self.bok_stat_code,[],self.log,BOK_STAT_CODE_DATA)
        return results 


    def get_all_stats_code(self):
        stats_code_list = list(self.bok_stat_code.find({},{"STAT_CODE":1}))
        stats_code_list =  list(set([ code['STAT_CODE'] for code in stats_code_list if code.get("STAT_CODE")]))
        return stats_code_list

    def insert_all_data(self):
        self.insert_metadata()
        stat_code_list = list(self.bok_stat_code.find({},{"STAT_CODE":1}))
        stat_code_list = [ st['STAT_CODE'] for st in stat_code_list]
        total_length = len(stat_code_list)
        jump = 10
        for start in range(0,total_length,jump):
            end = start + jump 
            insert_list = stat_code_list[start:end]
            self.insert_item_metadata_list(insert_list)



    def insert_item_metadata_list(self,stat_list):
        self.call_async_func('get_item_metadata',stat_list,callback_func="call_back_method")
        

    async def get_item_metadata(self,stat_code):
        url="http://ecos.bok.or.kr/api/StatisticItemList/"\
            f"{self.bank_secret_key}/json/kr/1/1000/{stat_code}"
        return await return_async_get(url)

        

    
    def call_back_method(self,future):
        metadata_result = future.result()
        result_json = metadata_result.get('data')
        if metadata_result['result'] == "fail":
            self.log.error(REQUEST_ERROR,extra= result_json)
            return False 
        if not result_json.get('StatisticItemList') or not result_json['StatisticItemList'].get("row"):
            self.log.error(NO_DATA,extra= result_json)
            return False 
        data_list = result_json['StatisticItemList']['row']
        results =bulk_mongo_inserts(data_list,self.bok_item_code,['STAT_CODE','ITEM_CODE'],self.log,BOK_META_DATA)
        return results 
        