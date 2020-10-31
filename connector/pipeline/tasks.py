import logging
import os
from typing import Dict
from pymongo import MongoClient
import pandas as pd
from celery_app import celery_app
from pipeline.datahandler import (
    prepare_stock_df,
    save_pipeline_data
)
from fp_types import (
    CONNECTED_FINANCIAL_STATEMENTS,
    NORMAL_FINANCIAL_STATEMENTS,
)


logger = logging.getLogger(__name__)


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
    normal_df = df[df['report_type'] == NORMAL_FINANCIAL_STATEMENTS]
    stockcode = ''.zfill(6-len(str(code))) + str(code)
    data_list = list(
        db.market_data.find(
            {"Code": stockcode}, 
            {"_id": False}
        )
    )
    stock_df = pd.DataFrame(data_list)
    stock_df = prepare_stock_df(stock_df)
    success_list = []
    failed_list = []
    try:
        save_pipeline_data(
            db,
            normal_df,
            stock_df,
            NORMAL_FINANCIAL_STATEMENTS,
            code
        )
        success_list.append(NORMAL_FINANCIAL_STATEMENTS)
    except ValueError as ve:
        failed_list.append(NORMAL_FINANCIAL_STATEMENTS)
        logger.error(ve)
    try:
        save_pipeline_data(
            db,
            connected_df,
            stock_df,
            CONNECTED_FINANCIAL_STATEMENTS,
            code
        )
        success_list.append(CONNECTED_FINANCIAL_STATEMENTS)
    except ValueError as ve:
        failed_list.append(CONNECTED_FINANCIAL_STATEMENTS)
        logger.error(ve)    

    client.close()
    return {
        'success_list': success_list,
        'failed_list': failed_list
    }