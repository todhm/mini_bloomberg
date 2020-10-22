import os
from typing import Dict
from pymongo import MongoClient
import pandas as pd
from celery_app import celery_app
from pipeline.datahandler import (
    prepare_report_data,
    prepare_stock_df
)
from fp_types import (
    CONNECTED_FINANCIAL_STATEMENTS,
    NORMAL_FINANCIAL_STATEMENTS,
    feature_list,

)


@celery_app.task(bind=True, name='save_machinelearing_features_data')
def save_machinelearing_features_data(self, code: str, db_name: str) -> Dict:
    key_list = [
        'cashflow_from_operation',
        'operational_income',
        'gross_profit',
        'book_value',
        'sales',
        'extra_ordinary_profit',
        'extra_ordinary_loss',
        'net_income',
        'total_assets',
        'longterm_debt',
        'current_assets',
        'current_debt',
        'code',
        'corp_name',
        'title',
        'reg_date',
        'report_link',
        'table_link',
        'period_type',
        'report_type'
    ]
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    success_list = list(
        db.report_data_list.find(
            {"code": code}, 
            {"_id": False}
        )
    )
    df = pd.DataFrame(success_list)
    df = df[key_list]
    df['reg_date'] = pd.to_datetime(df['reg_date'], infer_datetime_format=True)
    df = df.sort_values('reg_date', ascending=True)
    connected_df = df[df['report_type'] == CONNECTED_FINANCIAL_STATEMENTS]
    if len(connected_df) >= 1:
        df = connected_df
    else:
        df = df[df['report_type'] == NORMAL_FINANCIAL_STATEMENTS]
    stockcode = ''.zfill(6-len(str(code))) + str(code)
    data_list = list(
        db.market_data.find(
            {"Code": stockcode}, 
            {"_id": False}
        )
    )
    stock_df = pd.DataFrame(data_list)
    stock_df = prepare_stock_df(stock_df)
    df = prepare_report_data(df, stock_df)
    df = df.rename(columns={
        'yearly_code': 'code',
        'yearly_reg_date': 'reg_date',
        'yearly_corp_name': 'corp_name',
        'yearly_report_link': 'report_link'
    })
    save_feature_list = feature_list + [
        'code', 
        'reg_date', 
        'corp_name', 
        'report_link'
    ]
    df = df[save_feature_list]
    if len(df) < 1:
        raise ValueError("Failed to create machinelearning data")
    db.ml_feature_list.delete_many({'code': code})
    db.ml_feature_list.insert_many(df.to_dict('records'))
    client.close()
    return {'result': 'success'}