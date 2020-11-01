import logging
import os
from typing import Dict
from pymongo import MongoClient
from sklearn.ensemble import RandomForestRegressor
from celery_app import celery_app
from ml_models.datahandler import save_model_results
from fp_types import (
    CONNECTED_FINANCIAL_STATEMENTS,
    NORMAL_FINANCIAL_STATEMENTS,
)

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, name='save_ml_models')
def save_ml_models(
    self, db_name: str, model_name: str = 'randomforest'
) -> Dict:
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    try:
        model_params = {
            'n_estimators': 200,
            'max_depth': 10,
            'random_state': 0
        }
        regr = RandomForestRegressor(
            **model_params
        )
        model_prefix = f'{model_name}_connected'
        save_model_results(
            db=db,
            report_type=CONNECTED_FINANCIAL_STATEMENTS,
            model_prefix=model_prefix,
            regr=regr,
            model_params=model_params,
        )
    except Exception as e:
        error_message = (
            "Error while making models for "
            f"{CONNECTED_FINANCIAL_STATEMENTS} {str(e)}"
        )
        logger.error(error_message)
        raise ValueError(error_message)
    try:
        model_params = {
            'n_estimators': 200,
            'max_depth': 10,
            'random_state': 0
        }
        regr = RandomForestRegressor(
            **model_params
        )
        model_prefix = f'{model_name}_normal'
        save_model_results(
            db=db,
            report_type=NORMAL_FINANCIAL_STATEMENTS,
            model_prefix=model_prefix,
            regr=regr,
            model_params=model_params,
        )
    except Exception as e:
        error_message = (
            "Error while making models for "
            f"{NORMAL_FINANCIAL_STATEMENTS} {str(e)}"
        )
        logger.error(error_message)
        raise ValueError(error_message)
    client.close()
    return {
        "result": "success"
    }