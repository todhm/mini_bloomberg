import logging
import os
from datetime import datetime as dt
from typing import Dict, Optional, Literal
from math import ceil

from pymongo import MongoClient
import pandas as pd
from workalendar.asia import SouthKorea

from celery_app import celery_app
from pipeline.datahandler import (
    prepare_stock_df,
    save_pipeline_data
)
from fp_common.fp_types import (
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

    # Prepare Query
    report_query = {"code": code, "report_type": report_type}
    stockcode = ''.zfill(6-len(str(code))) + str(code)
    market_query = {"Code": stockcode}
    if market_date:
        market_date = dt.strptime(market_date, '%Y%m%d')
        ca = SouthKorea()
        market_start_date = ca.add_working_days(market_date, -11)
        market_start_date = dt(
            market_start_date.year, 
            market_start_date.month, 
            market_start_date.day
        )
        market_query['Date'] = {'$gte': market_start_date}

        two_years_days = ceil(365.25 * 2)
        report_start_date = ca.add_working_days(
            market_start_date, two_years_days * -1
        )
        report_start_date = dt(
            report_start_date.year, 
            report_start_date.month, 
            report_start_date.day
        )
        report_query['reg_date'] = {'$gte': report_start_date}

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
        error_message = (
            "Error while make ml data "
            f"{code} {report_type} {market_date} "
            f"{str(ve)}"
        )
        logger.error(error_message)
        raise ValueError(error_message)
    return {
        'result': 'success'
    }