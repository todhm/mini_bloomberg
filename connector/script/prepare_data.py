import json
from datetime import datetime as dt
from pymongo import MongoClient
import pandas as pd
from pipeline.tasks import save_machinelearing_features_data
from tests.lr_test_app import settings
import fp_types


def prepare_report_data():
    with open('./datacollections/report_data_list.json') as f:
        data_list = json.load(f)
    for data in data_list:
        data['reg_date'] = dt.strptime(data['reg_date'], '%Y-%m-%d')
    df = pd.read_csv('./datacollections/teststcoks.csv')
    df = df.drop(columns=['Unnamed: 0'])
    df['Code'] = df['Code'].apply(lambda x: ''.zfill(6-len(str(x))) + str(x))
    df['Date'] = df['Date'].apply(lambda x: dt.strptime(x, '%Y-%m-%d'))
    stock_data_list = df.to_dict('records')        
    client = MongoClient(settings.MONGO_URI)
    db = client[settings.MONGODB_NAME]
    db.report_data_list.drop()
    db.market_data.drop()
    db.report_data_list.insert_many(data_list)
    db.market_data.insert_many(stock_data_list)
    db.ml_feature_list.drop()
    _ = save_machinelearing_features_data(
        "2200", settings.MONGODB_NAME,
        fp_types.NORMAL_FINANCIAL_STATEMENTS,
    )
    
    _ = save_machinelearing_features_data(
        "2200", settings.MONGODB_NAME,
        fp_types.CONNECTED_FINANCIAL_STATEMENTS,
    )
    
    _ = save_machinelearing_features_data(
        "3490", settings.MONGODB_NAME, 
        fp_types.NORMAL_FINANCIAL_STATEMENTS
    )
    _ = save_machinelearing_features_data(
        "3490", settings.MONGODB_NAME, 
        fp_types.CONNECTED_FINANCIAL_STATEMENTS
    )
    client.close()

        
if __name__ == "__main__":
    prepare_report_data()