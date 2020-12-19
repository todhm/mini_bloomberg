import logging
import pytest
from datetime import datetime as dt
from tests.simulation_test_app import settings

from models.dataclass_models import MlModel, StockPossessData
from ml_models.tasks import (
    simulate_model_result,
    save_recommendation
)
from fp_common import fp_types


logger = logging.getLogger(__name__)


@pytest.mark.recommend
def test_recommendation(simulationmongo):
    # final_date = simulationmong
    model_name = 'test_randomforest_normal20201101224257.joblib'
    model_data = {
        'model_name': model_name,
        'report_type': fp_types.NORMAL_FINANCIAL_STATEMENTS,
        'test_code_list': [
            '2200',
        ]
    }
    possess_data = StockPossessData(
        count=10,
        price=10000,
        code='2200'

    )
    possess_json = possess_data.to_json
    ml_result = MlModel(**model_data)
    ml_result.save(simulationmongo.ml_model_result)
    _ = simulate_model_result(
        db_name=settings.MONGODB_NAME,
        model_name=ml_result.model_name
    )
    simulation_dict = simulationmongo.simulation_result.find_one(
        {'model_name': model_name}
    )
    portfolio_dict = {
        'initial_budget': 1000000,
        'current_budget': 1000000,
        'reg_date': dt.now(),
        'possess_list': [possess_json],
        'sell_list': [],
        'simulation_result': simulation_dict['_id']
    }
    simulationmongo.portfolio.insert_one(portfolio_dict)
    final_date = simulationmongo.ml_feature_list.find_one(
        {"report_type": fp_types.NORMAL_FINANCIAL_STATEMENTS},
        {'stock_date': 1},
        sort=[("stock_date", -1)]
    )
    final_date = final_date['stock_date']
    final_date = dt.strftime(final_date, '%Y%m%d')
    portfolio_id = str(portfolio_dict['_id'])
    save_recommendation(
        db_name=simulationmongo.name,
        stock_date=final_date,
        portfolio_id=portfolio_id
    )
    recommendation_object = simulationmongo.recommendation.find_one({
        'simulation_result': simulation_dict['_id']
    })
    assert(len(recommendation_object['sell_list']) > 0)
    assert(len(recommendation_object['buy_list']) > 0)