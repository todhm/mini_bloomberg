from utils.class_utils import DataHandlerClass
from utils.date_utils import daterange
from utils.api_utils import return_async_get
from utils.mongo_utils import bulk_mongo_inserts
from fp_types import REQUEST_ERROR,BULK_DATA_FAILED,NO_DATA, \
                    BULK_DATA_WRITE,BOK_META_DATA,BOK_STAT_DATA
from datahandler.metadatahandler import MetaDataHandler 
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
import functools
import asyncio
import aiohttp
import requests
import json 
import os 


class BokStatHandler(DataHandlerClass):

    def __init__(self,db_name='testdb',*args,**kwargs):
        super().__init__(db_name)
        self.bank_secret_key = os.environ.get("KOREAN_BANK_SECRET_KEY")
        self.bok_item_code = self.db.bok_item_code
        self.bok_stats = self.db.bok_stats
        self.mdh = MetaDataHandler(db_name)

    def return_desired_stats(self):
        return [
        '060Y001','028Y001','028Y022',
        '028Y007','027Y601','036Y001',
        '036Y002','036Y003','036Y004',
        '036Y005','036Y006','085Y013',
        '085Y014','102Y053','102Y037'
        ]
    
    def insert_stats_assigned(self,stats_list):
        item_code_list = list(self.bok_item_code.find({
            "STAT_CODE":{"$in":stats_list}},
            {"STAT_CODE":1,"ITEM_CODE":1}
        ))
        total_length = len(item_code_list)
        jump = 30
        result = []
        for start in range(0,total_length,jump):
            end = start + jump 
            insert_list = item_code_list[start:end]
            temp_result = self.insert_stat_list(insert_list)
            result.extend(temp_result)
        return result


    def insert_all_stat_list(self,is_first=False):
        item_code_list = list(self.bok_item_code.find({},{"STAT_CODE":1,"ITEM_CODE":1}))
        item_code_list = item_code_list[::-1]
        total_length = len(item_code_list)
        jump = 10
        for start in range(0,total_length,jump):
            end = start + jump 
            insert_list = item_code_list[start:end]
            self.insert_stat_list(insert_list,is_first)

    
    def call_back_method(self,stat_code,item_code,future):
        metadata_result = future.result()
        result_json = metadata_result.get('data')
        if metadata_result['result'] == "fail":
            self.log.error(REQUEST_ERROR,extra= result_json)
            return False 
        if not result_json.get('StatisticSearch') or not result_json['StatisticSearch'].get("row"):
            self.log.error(NO_DATA)
            return False 
        data_list = result_json['StatisticSearch']['row']
        for data in data_list:
            data['ORIGINAL_ITEM_CODE'] = item_code
        result = bulk_mongo_inserts(
            data_list,
            self.bok_stats,
            ['STAT_CODE','ORIGINAL_ITEM_CODE'],
            self.log,
            BOK_STAT_DATA   
        )
        return result
          

    def insert_stat_list(self,item_code_list,is_first=False):
        stats_code_list = [ 
            item_code['STAT_CODE'] 
            for item_code in  item_code_list if item_code.get("STAT_CODE")
        ]
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        async_list= []
        for data in item_code_list:
            item_code = data.get('ITEM_CODE')
            stat_code = data.get('STAT_CODE')
            item_data = list(self.bok_item_code.find({"ITEM_CODE":item_code},{"_id":False}))
            if not item_data:
                self.log.error(NO_DATA)
                continue
            item_data = item_data[0]
            start_time = item_data['START_TIME']
            end_time = item_data['END_TIME']
            cycle = item_data['CYCLE']
            data_cnt = item_data.get("DATA_CNT")
            task = loop.create_task(self.insert_stats(
                item_code,stat_code,data_cnt,cycle,start_time,end_time
            ))
            task.add_done_callback(
                functools.partial(self.call_back_method,stat_code,item_code))
            async_list.append(task)
        all_results = asyncio.gather(*async_list)
        result_list = loop.run_until_complete(all_results)
        loop.close()
        return result_list

        
    async def insert_stats(self,item_code,stat_code,data_cnt,cycle,start_time,end_time):
        url = 'http://ecos.bok.or.kr/api/StatisticSearch/{}/json/kr/0/{}/{}/{}/{}/{}/{}/'.format(
            self.bank_secret_key,data_cnt ,stat_code,cycle,
            start_time, end_time, item_code
        )
        return await return_async_get(url)


