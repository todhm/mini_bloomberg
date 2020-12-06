import logging
import os
from datetime import datetime as dt
from typing import Dict, Optional, Literal

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
def save_machinelearing_features_data(
    self, 
    code: str, 
    db_name: str, 
    report_type: Literal[
        NORMAL_FINANCIAL_STATEMENTS,
        CONNECTED_FINANCIAL_STATEMENTS
    ] = NORMAL_FINANCIAL_STATEMENTS,
    market_date: Optional[str] = None,
) -> Dict:
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
    report_query = {"code": code, "report_type": report_type}
    success_list = list(
        db.report_data_list.find(
            report_query, 
            {"_id": False}
        )
    )
    df = pd.DataFrame(success_list)
    df = df[key_list]
    df['reg_date'] = pd.to_datetime(df['reg_date'], infer_datetime_format=True)
    df = df.sort_values('reg_date', ascending=True)
    stockcode = ''.zfill(6-len(str(code))) + str(code)
    market_query = {"Code": stockcode}
    if market_date:
        market_date = dt.strptime(market_date, '%Y%m%d')
        market_query['Date'] = {'$gte': market_date}
    data_list = list(
        db.market_data.find(
            market_query, 
            {"_id": False}
        )
    )
    stock_df = pd.DataFrame(data_list)
    stock_df = prepare_stock_df(stock_df)
    try:
        save_pipeline_data(
            db,
            df,
            stock_df,
            report_type,
            code,
            market_date=market_date,
        )
        client.close()
    except ValueError as ve:
        client.close()
        raise ve
    return {
        'result': 'success'
    }