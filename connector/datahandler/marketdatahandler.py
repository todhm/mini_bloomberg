import os
import datetime
from datetime import datetime as dt
import pandas as pd
from pymongo import MongoClient
from utils.api_utils import async_post_functions
from utils.common import chunks

        
class MarketDataHandler(object): 

    def __init__(self, dbName='fp_data'):
        self.client = MongoClient(os.environ.get("MONGO_URI"))
        self.db = self.client[dbName]
        
    def __del__(self):
        self.client.close()
        print("close mongo connection")

    def prepare_marcap_data(
        self,
        start,
        end=None,
        code=None,
        dataloc='datacollections/data',
    ):
        '''
        Excel로 저장된 marcap data엑셀에 저장하기
        '''
        end = start if end is None else end
        df_list = []

        dtypes = {
            'Code':str, 'Name':str, 'Open':float, 
            'High':float, 'Low':float, 'Close':float, 
            'Volume':float, 'Amount':float,
            'Changes':float, 'ChangesRatio':float, 
            'Marcap':float, 'Stocks':float, 'MarcapRatio': float, 
            'ForeignShares':float, 'ForeignRatio':float, 'Rank':float
        }

        for year in range(start, end + 1):
            try:
                csv_file = f'{dataloc}/marcap-{year}.csv.gz'
                print(csv_file)
                df = pd.read_csv(csv_file, dtype=dtypes, parse_dates=['Date'])
                df_list.append(df)
            except Exception as e:
                print(e)
                pass
        df_merged = pd.concat(df_list)
        min_date = df_merged['Date'].min()
        max_date = df_merged['Date'].max()
        print(min_date, max_date)
        data_list = df_merged.to_dict('records')
        print(type(data_list[0]['Date']))
        self.db.market_data.insert_many(data_list)

    def create_market_data(
        self,
        dataloc='datacollections/data',
    ):
        '''
        Excel로 저장된 marcap data엑셀에 저장하기
        '''
        start_year = 1995
        current_year = dt.now().year

        for year in range(start_year, current_year + 1):
            try:
                self.prepare_marcap_data(year)
            except Exception as e: 
                raise ValueError(f"Error while making data on year {year}" + str(e))

    def save_api_market_data(self, startdate, enddate):
        date_string_list = [
            dt.strftime(startdate + datetime.timedelta(days=x), '%Y%m%d')
            for x in range(0, (enddate-startdate).days)
        ]
        jump = 30
        for start in range(0, len(date_string_list), jump):
            end = start + jump
            request_list = date_string_list[start:end]
            request_list = [
                {'dateList': request_data} 
                for request_data in chunks(request_list, 3)
            ]
            link_url = os.environ.get("FPCRAWLER_URL")
            url = f'{link_url}/return_market_data'
            success_list, failed_list = async_post_functions(url, request_list)
            total_result = []
            for success_data in success_list:
                for inner_data in success_data:
                    for x in inner_data: 
                        x['Date'] = pd.to_datetime(
                            x['Date'], 
                            format='%Y-%m-%d', 
                            errors='raise'
                        )
                        total_result.append(x)
            if total_result:
                min_date = min(total_result, key=lambda x: x['Date'])
                max_date = max(total_result, key=lambda x: x['Date'])
                self.db.market_data.delete_many(
                    {'Date': {"$gte": min_date, "$lte": max_date}}
                )
                self.db.market_data.insert_many(total_result)

                        
                        

                



                    


                
                

            
            


