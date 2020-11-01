import random
from datetime import datetime as dt
from math import ceil
from fp_types import (
    CONNECTED_FINANCIAL_STATEMENTS,
    NORMAL_FINANCIAL_STATEMENTS,
    feature_list
)
from typing import (
    List,
    Dict,
    Tuple,
    Literal
)
from models.dataclass_models import MlModel
from sklearn.base import BaseEstimator
from sklearn.metrics import mean_squared_error
from pymongo.database import Database
import pandas as pd
import pickle


def return_test_code_list(
    data_list: List[Dict]
) -> Tuple[List, List]:
    code_list = list(set([x['code'] for x in data_list]))
    train_percentage = 0.65
    select_numbers = ceil(len(code_list) * train_percentage)
    if len(code_list) <= 2:
        select_numbers = 1
    train_code_list = random.choices(
        code_list,
        k=select_numbers
    )
    test_code_list = list(set(code_list) - set(train_code_list))
    return train_code_list, test_code_list
    

def save_model_results(
    db: Database,
    report_type: Literal[
        CONNECTED_FINANCIAL_STATEMENTS,
        NORMAL_FINANCIAL_STATEMENTS
    ],
    model_prefix: str,
    regr: BaseEstimator,
    model_params: Dict = {}
):
    data_list = list(
        db.ml_feature_list.find(
            {"report_type": report_type},
            {"_id": False}
        )
    )
    train_code_list, test_code_list = return_test_code_list(
        data_list
    )
    df = pd.DataFrame(data_list)
    df_train = df[df['code'].isin(train_code_list)]
    df_test = df[df['code'].isin(test_code_list)]
    x_train = df_train[feature_list]
    y_train = df_train['Close']
    x_test = df_test[feature_list]
    y_test = df_test['Close']
    now_time_str = dt.strftime(
        dt.now(),
        "%Y%m%d%H%M%S"
    )
    regr.fit(x_train, y_train)
    model_name = f'{model_prefix}{now_time_str}.joblib'
    with open(f'ml_models/mlfiles/{model_name}', 'wb') as f:
        pickle.dump(regr, f)
    y_predict = regr.predict(x_test)
    current_mse = mean_squared_error(
        y_test,
        y_predict 
    )
    model_performance = {
        'MSE': current_mse,
    }
    mldata = MlModel(
        model_name=model_name,
        model_performance=model_performance,
        model_features=feature_list,
        train_code_list=train_code_list,
        test_code_list=test_code_list,
        model_params=model_params,
    )
    data = mldata.to_json
    db.ml_model_result.insert_one(data)
    return data
    
    



