import pandas as pd
from pandasql import sqldf
import FinanceDataReader as fdr
from pandasql import sqldf
import numpy as np
from fp_types import (
    NORMAL_FINANCIAL_STATEMENTS,
    YEARLY_REPORT
)


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
    df = df.sort_values('reg_date', ascending=False)
    df['report_year'] = df['reg_date'].apply(lambda x: x.year)
    year_list = list(range(df.report_year.min(), df.report_year.max()))
    df = df.set_index('report_year')
    df = df.reindex(year_list)
    df['future_reg_date'] = df['reg_date'].shift(-1)
    df['second_future_reg_date'] = df['reg_date'].shift(-2)
    df['eq_offer'] = df['reg_date'].apply(
        lambda x: check_eq_offer_availiable(x, eq_df)
    )
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
    df['reg_date'] = df['reg_date'].dt.tz_convert('Asia/Seoul')
    return df


def create_f_score_data(joined_df):
    joined_df['market_value'] = joined_df['Volume'] * joined_df['Close']
    joined_df['book_to_market'] = joined_df['book_value']\
        / joined_df['market_value']
    joined_df['log_book_to_market'] = (
        np.log(joined_df['book_value']/ joined_df['market_value'])
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
    joined_df['last_year_current_ratio'] = joined_df['last_year_current_assets'] \
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
    joined_df['f_turn'] = ((joined_df['atr'] - joined_df['last_year_atr']) > 0).apply(lambda x: int(x))
    joined_df['f_sum'] = (
        joined_df['f_roa'] + joined_df['f_cfo'] + joined_df['f_roa_diff'] + joined_df['f_accrual']
        + joined_df['f_lever'] + joined_df['f_liquid'] + joined_df['f_eq'] + joined_df['f_gmo']
        + joined_df['f_turn']
    )
    joined_df = joined_df.set_index('date')
    return joined_df


def return_local_sql(q,loc_object):
    return sqldf(q,loc_object)

    
def create_machine_learning_data(
        success_list: list,
        eq_offer_list: list,
        stock_df: pd.DataFrame,
        report_type: str = NORMAL_FINANCIAL_STATEMENTS,
        period_type: str = YEARLY_REPORT,
        ) -> pd.DataFrame:
    data_list = map(extract_necessary_data, success_list)
    df = pd.DataFrame(data_list)
    df = df[
        (df['period_type'] == period_type)
        & (df['report_type'] == report_type)]
    eq_df = pd.DataFrame(eq_offer_list)
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
            stock_df.date >df.reg_date and stock_df.date <= df.future_reg_date

    '''

    # Now, get your queries results as dataframe using the sqldf object that you created
    joined_df = pysqldf(cond_join, locals())
    joined_df = create_f_score_data(joined_df)
    return joined_df


def return_pipeline_data(code, success_list, offer_list):
    code = str(code)
    stockcode = ''.zfill(6-len(code)) + code
    stock_df = fdr.DataReader(stockcode, '19990101')
    converted_df = create_machine_learning_data(success_list, offer_list, stock_df)
    converted_df = converted_df.dropna(subset=['last_year_atr'])
    return converted_df


    