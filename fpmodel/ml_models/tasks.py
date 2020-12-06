import logging
import os
from typing import Dict, List
from pymongo import MongoClient
from sklearn.ensemble import RandomForestRegressor
from celery_app import celery_app
from ml_models.datahandler import save_model_results
from ml_models.simulation_handler import SimulationHandler
from fp_types import (
    CONNECTED_FINANCIAL_STATEMENTS,
    NORMAL_FINANCIAL_STATEMENTS,
)

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, name='save_ml_models')
def save_ml_models(
    self, db_name: str, model_name: str = 'randomforest'
) -> List[Dict]:
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    model_result = []
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
        result = save_model_results(
            db=db,
            report_type=CONNECTED_FINANCIAL_STATEMENTS,
            model_prefix=model_prefix,
            regr=regr,
            model_params=model_params,
        )
        if result.get('_id'):
            result.pop("_id")
        model_result.append(result)
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
        result = save_model_results(
            db=db,
            report_type=NORMAL_FINANCIAL_STATEMENTS,
            model_prefix=model_prefix,
            regr=regr,
            model_params=model_params,
        )
        if result.get('_id'):
            result.pop("_id")
        model_result.append(result)
    except Exception as e:
        error_message = (
            "Error while making models for "
            f"{NORMAL_FINANCIAL_STATEMENTS} {str(e)}"
        )
        logger.error(error_message)
        raise ValueError(error_message)
    client.close()
    return model_result


@celery_app.task(bind=True, name='simulate_model_result')
def simulate_model_result(
    self, db_name: str, model_name: str = 'randomforest'
) -> Dict:
    mongo_uri = os.environ.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    try:
        sml = SimulationHandler(
            db=db,
            model_name=model_name,
            minmum_purchase_diff=0.15,
            maximum_sell_diff=0.01,
            initial_total_budget=3000000,
            single_purchase_amount=20
        )
        result = sml.simulate_model_result()
    except Exception as e:
        error_message = (
            "Error while making simulation "
            f"{str(e)}"
        )
        logger.error(error_message)
        client.close()
        raise e
    client.close()
    return result
