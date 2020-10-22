from datetime import datetime as dt
from flask.cli import AppGroup
from mongo_models import models
import pandas as pd
from datahandler.marketdatahandler import MarketDataHandler
from datahandler import reportdatahandler
from datahandler.pipelinehandler import PipelineDataHandler
import random


index_cli = AppGroup('index')


@index_cli.command("create-index")
def create_index():
    md = models.MarketData(Code='test')
    md.save()
    md.delete()


@index_cli.command("create-data")
def create_total_data():
    mdh = MarketDataHandler(dbName='fp_data')
    mdh.create_market_data()


@index_cli.command("create-report-data")
def create_report_data():
    df = pd.read_excel('./datacollections/korean_company_list.xlsx')
    df = df.fillna('')
    df = df.sort_values('register_date', ascending=True)
    df['register_date'] = df['register_date'].apply(
        lambda x: x.strftime('%Y%m%d')
    )
    data_list = df.to_dict('records')
    reportdatahandler.insert_company_data_list(data_list, db_name='fp_data')


@index_cli.command("best-data")
def fetch_best_data():
    dbName = 'fp_data'
    pdh = PipelineDataHandler(db_name=dbName)
    search_date = dt.strptime('20200102', '%Y%m%d')
    data_list = pdh.return_best_code_data(
        search_date=search_date, data_limit=10
    )
    for data in data_list:
        print(data['f_sum'], data['code'], data['book_to_market'])


@index_cli.command("prepare_random_list")
def prepare_random_list():
    df = pd.read_excel('./datacollections/korean_company_list.xlsx')
    df = df.fillna('')
    df = df.sort_values('register_date', ascending=True)
    df['register_date'] = df['register_date'].apply(
        lambda x: x.strftime('%Y%m%d')
    )
    data_list = df.to_dict('records')
    print(random.sample(data_list, 10))
