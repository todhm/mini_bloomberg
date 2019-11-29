import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from datahandler.quandldatahandler import QuandlDataHandler
import json 

with open('datacollections/commodity.json') as f:
    commodity_json = json.load(f)

commodity_list = [cm['code'] for cm in commodity_json]
db_name='fp_data'
qdh = QuandlDataHandler(db_name)
qdh.insert_list(commodity_list,'meta')
qdh.insert_list(commodity_list,'stats')
qdh.close()