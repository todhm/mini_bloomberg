import os
from typing import List
from datetime import datetime as dt
import pandas as pd
from pymongo.collection import Collection
import requests


def save_api_market_data(date_list: List[dt], market_data: Collection):
    date_string_list = [
        dt.strftime(x, '%Y%m%d')
        for x in date_list
    ]
    jump = 10
    for start in range(0, len(date_string_list), jump):
        end = start + jump
        request_list = date_string_list[start:end]
        link_url = os.environ.get("FPCRAWLER_URL")
        url = f'{link_url}/return_market_data'
        post_data = {'dateList': request_list}
        response = requests.post(url, json=post_data)
        if response.status_code != 200:
            if response.status_code == 400:
                error_message = response.json()['message']
            else:
                error_message = response.text
            print("Error making reqeusts", post_data, error_message)
            raise ValueError("Error making requests " + error_message)
        success_data = response.json()
        total_result = []
        for inner_data in success_data:
            for x in inner_data: 
                x['Date'] = pd.to_datetime(
                    x['Date'], 
                    format='%Y-%m-%d', 
                    errors='raise'
                )
                total_result.append(x)
        print(len(total_result), 'length of data crawled')
        if total_result:
            min_date = min(total_result, key=lambda x: x['Date'])
            max_date = max(total_result, key=lambda x: x['Date'])
            market_data.delete_many(
                {'Date': {"$gte": min_date, "$lte": max_date}}
            )
            market_data.insert_many(total_result)

                    
                    

            



                


            
            

        
        


