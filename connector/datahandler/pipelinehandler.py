from datetime import datetime as dt
from datetime import timedelta
from  itertools import groupby
import os
import pandas as pd
from pandasql import sqldf
from pymongo import MongoClient
import fp_types
import numpy as np


def extract_necessary_data(x):
    data = {}
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
        'reporter',
        'total_stock_count',
        'title',
        'total_assets',
        'reg_date',
        'report_link',
        'table_link',
        'period_type',
        'report_type'
    ]
    for key in key_list:
        data[key] = x.get(key)
    return data


def check_eq_offer_availiable(x, eq_df):
    for ts in eq_df['reg_date']:
        if (x-ts).days > 0 and (x-ts).days <= 365:
            return 1
    return 0


def prepare_report_data(df, eq_df):
    df['reg_date'] = pd.to_datetime(df['reg_date'], infer_datetime_format=True)  
    df = df.sort_values('reg_date', ascending=True)
    df['future_reg_date'] = df['reg_date'].shift(-1)
    df['second_future_reg_date'] = df['reg_date'].shift(-2)
    df = df[
        df['future_reg_date'].isna() | 
        (df['future_reg_date'] - df['reg_date']).apply(lambda x:x.days < 365)
    ]
    df['report_year'] = df['reg_date'].apply(lambda x: x.year)
    df = df.drop_duplicates(subset=['report_year'], keep='first')
    if len(eq_df) > 0:
        df['eq_offer'] = df['reg_date'].apply(
            lambda x: check_eq_offer_availiable(x, eq_df)
        )
    else:
        df['eq_offer'] = 0
    df['last_year_assets'] = df['total_assets'].shift(1)
    df['two_year_before_assets'] = df['total_assets'].shift(2)
    df['last_year_longterm_debt'] = df['longterm_debt'].shift(1)
    df['last_year_current_debt'] = df['current_debt'].shift(1)
    df['last_year_net_income'] = df['net_income'].shift(1)
    df['last_year_extra_ordinary_loss'] = df['extra_ordinary_loss'].shift(1)
    df['last_year_extra_ordinary_profit'] = df['extra_ordinary_profit'].shift(1)
    df['last_year_current_assets'] = df['current_assets'].shift(1)
    df['last_year_sales'] = df['sales'].shift(1)
    df['last_year_gross_profit'] = df['gross_profit'].shift(1)
    df['reg_date'] = (
        pd.DatetimeIndex(df['reg_date'])
        .tz_localize('Asia/Seoul')
        .tz_convert('Asia/Seoul')
    )
    return df

def create_f_score_data(joined_df):
    joined_df['book_to_market'] = joined_df['book_value']\
        / joined_df['Marcap']
    joined_df['log_book_to_market'] = (
        np.log(joined_df['book_value']/ joined_df['Marcap'])
    )
    joined_df['roa'] = (
        joined_df['net_income']
        + joined_df['extra_ordinary_loss']
        - joined_df['extra_ordinary_profit']
        ) / joined_df['last_year_assets']
    joined_df['last_year_roa'] = (
        joined_df['last_year_net_income'] 
        + joined_df['last_year_extra_ordinary_loss']
        - joined_df['last_year_extra_ordinary_profit']
        ) / joined_df['two_year_before_assets']
    joined_df['f_roa'] = (joined_df['roa'] > 0).apply(lambda x: int(x))
    joined_df['cfo'] = joined_df['cashflow_from_operation'] / \
        joined_df['last_year_assets']
    joined_df['f_cfo'] = (joined_df['cfo'] > 0).apply(lambda x: int(x))
    joined_df['f_roa_diff'] = (
        (
            joined_df['roa'] - joined_df['last_year_roa']) > 0
    ).apply(lambda x: int(x))
    joined_df['accurual'] = (
        joined_df['net_income']
        + joined_df['extra_ordinary_loss']
        - joined_df['extra_ordinary_profit']
        - joined_df['cashflow_from_operation']
        ) / joined_df['last_year_assets']
    joined_df['f_accrual'] = (joined_df['accurual'] < 0).apply(lambda x: int(x))
    joined_df['leverage'] = joined_df['longterm_debt'] / (
        (joined_df['total_assets'] + joined_df['last_year_assets']) / 2)
    joined_df['leverage_last_year'] = joined_df['last_year_longterm_debt'] / (
        (joined_df['last_year_assets'] + joined_df['two_year_before_assets'])
        / 2
        )
    joined_df['f_lever'] = ((
        joined_df['leverage']
        - joined_df['leverage_last_year']
        ) < 0).apply(lambda x: int(x))
    joined_df['current_ratio'] = joined_df['current_assets']\
        / joined_df['current_debt']
    joined_df['last_year_current_ratio'] = joined_df['last_year_current_assets']\
        / joined_df['last_year_current_debt']
    joined_df['f_liquid'] = ((
            joined_df['current_ratio'] - joined_df['last_year_current_ratio']
        ) > 0).apply(lambda x: int(x))
    joined_df['f_eq'] = (joined_df['eq_offer'] == 1).apply(lambda x: int(x))
    joined_df['gmo'] = joined_df['gross_profit'] / joined_df['sales']
    joined_df['last_year_gmo'] = joined_df['last_year_gross_profit'] \
        / joined_df['last_year_sales']
    joined_df['delta_gmo'] = joined_df['gmo'] - joined_df['last_year_gmo']
    joined_df['f_gmo'] = (joined_df['delta_gmo'] > 0).apply(lambda x: int(x))
    joined_df['atr'] = joined_df['sales']/joined_df['last_year_assets']
    joined_df['last_year_atr'] = joined_df['last_year_sales'] \
        / joined_df['two_year_before_assets']
    joined_df['f_turn'] = (
        (joined_df['atr'] - joined_df['last_year_atr']) > 0
    ).apply(lambda x: int(x))
    joined_df['f_sum'] = (
        joined_df['f_roa'] + joined_df['f_cfo'] + joined_df['f_roa_diff'] 
        + joined_df['f_accrual'] + joined_df['f_lever'] + joined_df['f_liquid'] 
        + joined_df['f_eq'] + joined_df['f_gmo'] + joined_df['f_turn']
    )
    joined_df = joined_df.set_index('date')
    return joined_df


def return_local_sql(q, loc_object):
    return sqldf(q, loc_object)

    
def create_machine_learning_data(
        success_list: list,
        eq_offer_list: list,
        stock_df: pd.DataFrame,
) -> pd.DataFrame:
    data_list = map(extract_necessary_data, success_list)
    df = pd.DataFrame(data_list)
    eq_df = pd.DataFrame(eq_offer_list)
    if eq_offer_list:
        eq_df['reg_date'] = pd.to_datetime(eq_df['reg_date'])
    df = prepare_report_data(df, eq_df)
    stock_df['date'] = pd.to_datetime(
        stock_df.index.tz_localize('Asia/Seoul'),
        format='%Y-%m-%d %H:%M:%S'
    ).tz_convert('Asia/Seoul')
    stock_df = stock_df.reset_index(drop=True)
    pysqldf = return_local_sql
    cond_join = '''
        select
            stock_df.*,
            df.*
        from stock_df
        join df
        on
            stock_df.date >df.reg_date and (
                stock_df.date <= df.future_reg_date 
                or df.future_reg_date is null
            )

    '''
    # Now, get your queries results as dataframe using the sqldf object that you created
    joined_df = pysqldf(cond_join, locals())
    joined_df = create_f_score_data(joined_df)
    return joined_df


def return_pipeline_data(
    code,  
    db_name='fp_data', 
    period_type=fp_types.YEARLY_REPORT,
    search_date=''
):
    code = str(code)
    mongo_uri = os.environ.get("MONGO_URI", "localhost:27017")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    success_list = list(
        db.report_data_list.find(
            {"code": code, 'period_type': period_type}, 
            {"_id": False}
        )
    )
    offer_list = list(
        db.eq_offer_list.find(
            {"code": code}, 
            {"_id": False}
        )
    )
    success_list = sorted(success_list, key=lambda x: x['report_link'])
    final_list = filter_best_report(success_list)
    stockcode = ''.zfill(6-len(code)) + code
    data_list = list(db.market_data.find({"Code": stockcode}, {"_id": False}))
    client.close()
    stock_df = pd.DataFrame(data_list)
    stock_df = stock_df.set_index('Date')       
    converted_df = create_machine_learning_data(
        final_list,
        offer_list,
        stock_df
    )
    converted_df = converted_df.dropna(subset=['last_year_atr'])
    return converted_df


# 같은 리포트에 연결재무제표가있으면 연결재무제표를 없으면 일반 재무제표를 반환할 수 있게 만드는 함수
def filter_best_report(report_data_list):
    report_data_list = sorted(report_data_list, key=lambda x: x['report_link'])
    final_list = []
    for report_link, report_data in groupby(
        report_data_list, 
        key=lambda x: x['report_link']
    ):
        connected_report_data = filter(
            lambda x: (
                x['report_type'] == "CONNECTED_FINANCIAL_STATEMENTS"
            ),
            report_data
        )
        report_data = list(report_data)
        connected_report_data = list(connected_report_data)
        if connected_report_data:
            final_list.append(connected_report_data[0])
        else:
            final_list.append(report_data[0])
    return final_list


class PipelineDataHandler(object):

    def __init__(self, db_name='fp_data'):
        self.db_name = db_name
        mongo_uri = os.environ.get("MONGO_URI", "localhost:27017")
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.period_type = fp_types.YEARLY_REPORT

    def return_best_code_data(self, search_date=None, data_limit=10):
        market_list = self.return_best_data(
            search_date=search_date, 
            data_limit=data_limit
        )
        report_form_code_list = [
            code_data['code'] for code_data in market_list
        ]
        start_date = search_date - timedelta(days=365*3)
        next_date = search_date + timedelta(days=1)
        success_list = list(self.db.report_data_list.find(
            {
                'period_type': self.period_type,
                'code': {"$in": report_form_code_list},
                'reg_date': {"$gte": start_date, "$lt": next_date}
            }, 
            {
                "_id": False, 
            }
        ))
        success_list = filter_best_report(success_list)
        offer_list = list(
           self.db.eq_offer_list.find(
                {
                    "code": {"$in": report_form_code_list},
                }, 
                {"_id": False}
            )
        )
        df_list = []
        for code in report_form_code_list:
            filtered_success_list = list(filter(
                lambda x: x['code'] == code,
                success_list
            ))
            filtered_offer_list = list(filter(
                lambda x: x['code'] == code, 
                offer_list
            ))
            filtered_market_list = list(filter(
                lambda x: x['code'] == code, 
                market_list
            ))
            stock_df = pd.DataFrame(filtered_market_list)            
            stock_df = stock_df[[
                'Date',
                'Code', "Name", 
                'Marcap', 'Stocks'
            ]]
            stock_df = stock_df.set_index('Date')       
            df = create_machine_learning_data(
                filtered_success_list,
                filtered_offer_list,
                stock_df
            )
            df_list.append(df)
        return df_list 

    def return_best_data(
        self,
        search_date=None,
        data_limit=10
    ):
        start_date = search_date - timedelta(days=365)
        next_date = search_date + timedelta(days=1)
        report_data_list = list(
            self.db.report_data_list.find(
                {
                    'period_type': self.period_type,
                    'reg_date': {"$gte": start_date, "$lt": search_date}
                }, 
                {
                    "_id": False, 
                    'book_value': 1, 
                    'code': 1,
                    'reg_date': 1, 
                    'report_link': 1, 
                    'report_type': 1
                }
            )
        )
        market_data_list = list(self.db.market_data.find(
            {
                'Date': {
                    "$gte": search_date, '$lt': next_date
                }
            },
            {"_id": False}
        ))
        final_list = filter_best_report(report_data_list)
        report_df = pd.DataFrame(final_list)
        market_df = pd.DataFrame(market_data_list)
        report_df['newCode'] = (
            report_df['code'].apply(lambda x: ''.zfill(6-len(x)) + x)
        )
        report_df = report_df.drop_duplicates(subset=['code'], keep='first')
        joined_df = report_df.merge(
            market_df, 
            how='inner', 
            left_on='newCode', 
            right_on='Code'
        )
        joined_df['book_to_market'] = (
            joined_df['book_value'] / joined_df['Marcap']
        )
        return joined_df.sort_values(
            ['book_to_market'],
            ascending=False
        )[:data_limit].to_dict('records')
