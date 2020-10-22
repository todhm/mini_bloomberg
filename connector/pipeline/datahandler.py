import pandas as pd
from pandasql import sqldf
from fp_types import (
    YEARLY_REPORT,
    MARCH_REPORT,
    SEPTEMBER_REPORT,
    SEMINUAL_REPORT
)


def return_local_sql(q, loc_object):
    return sqldf(q, loc_object)


def prepare_stock_df(stock_df: pd.DataFrame) -> pd.DataFrame:
    stock_df = stock_df.set_index('Date')       
    lags = [1, 5, 10]
    for lag in lags:
        stock_df[f'return_{lag}d'] = (
            stock_df[['Close']]
            .pct_change(lag)
            .add(1)
            .pow(1/lag)
            .sub(1)
        )
    return stock_df


def prepare_period_numerics(df: pd.DataFrame, period: str) -> pd.DataFrame:
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
    rename_dict = {}
    for key in key_list:
        rename_dict[key] = f'{period}_{key}'
    df = df.rename(columns=rename_dict)
    df['reg_date'] = df[f'{period}_reg_date']
    df = df.sort_values('reg_date', ascending=True)
    df[f'{period}_report_year'] = df['reg_date'].apply(lambda x: x.year)
    df = df.drop_duplicates(subset=[f'{period}_report_year'], keep='first')
    df.index = list(df[f'{period}_report_year'])
    df[f'last_{period}_assets'] = df[f'{period}_total_assets'].shift(1)
    df[f'last_{period}_longterm_debt'] = df[f'{period}_longterm_debt'].shift(1)
    df[f'last_{period}_current_debt'] = df[f'{period}_current_debt'].shift(1)
    df[f'last_{period}_net_income'] = df[f'{period}_net_income'].shift(1)
    df[f'last_{period}_extra_ordinary_loss'] = (
        df[f'{period}_extra_ordinary_loss'].shift(1)
    )
    df[f'last_{period}_extra_ordinary_profit'] = (
        df[f'{period}_extra_ordinary_profit'].shift(1)
    )
    df[f'last_{period}_current_assets'] = (
        df[f'{period}_current_assets'].shift(1)
    )
    df[f'last_{period}_sales'] = df[f'{period}_sales'].shift(1)
    df[f'last_{period}_gross_profit'] = df[f'{period}_gross_profit'].shift(1)
    df[f'{period}_roa'] = (
        df[f'{period}_net_income']
        + df[f'{period}_extra_ordinary_loss']
        - df[f'{period}_extra_ordinary_profit']
    ) / df[f'{period}_total_assets']
    df[f'last_{period}_roa'] = (
        df[f'last_{period}_net_income'] 
        + df[f'last_{period}_extra_ordinary_loss']
        - df[f'last_{period}_extra_ordinary_profit']
        ) / df[f'last_{period}_assets']
    df[f'{period}_cfo'] = (
        df[f'{period}_cashflow_from_operation']
        / df[f'{period}_total_assets']
    ) 
    df[f'{period}_roa_diff'] = df[f'{period}_roa'] - df[f'last_{period}_roa']
    df[f'{period}_accrual'] = (
        df[f'{period}_net_income']
        + df[f'{period}_extra_ordinary_loss']
        - df[f'{period}_extra_ordinary_profit']
        - df[f'{period}_cashflow_from_operation']
        ) / df[f'{period}_total_assets']
    df[f'{period}_leverage'] = (
        df[f'{period}_longterm_debt'] / df[f'{period}_total_assets']
    )
    df[f'{period}_leverage_last_year'] = (
        df[f'last_{period}_longterm_debt'] / df[f'last_{period}_assets']
    )
    df[f'{period}_leverage_diff'] = (
        df[f'{period}_leverage'] - df[f'{period}_leverage_last_year']
    )
    df[f'{period}_current_ratio'] = (
        df[f'{period}_current_assets'] / df[f'{period}_current_debt']
    )
    df[f'last_{period}_current_ratio'] = (
        df[f'last_{period}_current_assets'] / df[f'last_{period}_current_debt']
    )
    df[f'{period}_liquid'] = (
        df[f'{period}_current_ratio'] - df[f'last_{period}_current_ratio']
    )
    df[f'{period}_gmo'] = df[f'{period}_gross_profit'] / df[f'{period}_sales']
    df[f'last_{period}_gmo'] = (
        df[f'last_{period}_gross_profit'] 
        / 
        df[f'last_{period}_sales']
    )
    df[f'{period}_delta_gmo'] = df[f'{period}_gmo'] - df[f'last_{period}_gmo']
    df[f'{period}_atr'] = df[f'{period}_sales'] / df[f'{period}_total_assets']
    df[f'last_{period}_atr'] = (
        df[f'last_{period}_sales'] 
        / df[f'last_{period}_assets']
    )
    
    df[f'{period}_turonver'] = (
        df[f'{period}_atr'] - df[f'last_{period}_atr']
    )
    col_list = [
        f'{period}_sales',
        f'{period}_net_income',
        f'{period}_cashflow_from_operation',
        f'{period}_book_value',
        f'{period}_roa',
        f'{period}_cfo',
        f'{period}_roa_diff',
        f'{period}_accrual',
        f'{period}_leverage',
        f'{period}_leverage_diff',
        f'{period}_current_ratio',
        f'{period}_liquid',
        f'{period}_gmo',
        f'{period}_delta_gmo',
        f'{period}_turonver',
        f'{period}_atr',
        'reg_date'
    ]
    period_key_list = [f'{period}_{x}' for x in key_list]
    col_list.extend(period_key_list)
    df = df[col_list]
    return df
    
    
def prepare_report_data(df: pd.DataFrame, stock_df: pd.DataFrame):
    report_dict = {
        'yearly': YEARLY_REPORT, 
        'march': MARCH_REPORT,
        'september': SEPTEMBER_REPORT, 
        'june': SEMINUAL_REPORT
    }
    joined_df = stock_df
    joined_df['Marcap_lag'] = joined_df['Marcap'].shift(1)
    for period in ['yearly', 'march', 'september', 'june']:
        period_df = df[df['period_type'] == report_dict[period]]
        period_df = prepare_period_numerics(period_df, period)
        pysqldf = return_local_sql
        cond_join = '''
            select
                joined_df.*,
                period_df.*
            from joined_df
            join period_df
            on
            joined_df.date > period_df.reg_date 
            and (julianday(joined_df.date) - julianday(period_df.reg_date)) 
            < 365
        '''
        # Now, get your queries results as dataframe using the sqldf object 
        # that you created
        joined_df = pysqldf(cond_join, locals())
        joined_df[f'{period}_book_to_market'] = (
            joined_df[f'{period}_book_value'] / joined_df['Marcap_lag']
        )
        joined_df = joined_df.dropna(subset=[f'{period}_roa_diff'])
    return joined_df


