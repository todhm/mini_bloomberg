import pytest
from tests.simulation_test_app import settings
from models.dataclass_models import (
    MlModel,
    Simulation
)
from fp_common.fp_types import (
    NORMAL_FINANCIAL_STATEMENTS,
    CONNECTED_FINANCIAL_STATEMENTS
)
from ml_models.tasks import (
    save_ml_models,
    simulate_model_result,
)


@pytest.mark.simulation
@pytest.mark.mlmodel
@pytest.mark.celerylocal
def test_save_ml_models(simulationmongo):
    result = save_ml_models(
        db_name=settings.MONGODB_NAME,
        model_name='test_randomforest'
    )
    assert len(result) == 2
    result_list = list(
        simulationmongo.ml_model_result.find(
           {}, {"_id": False}
        )
    )
    assert len(result_list) == 2
    for result in result_list:
        ml_result = MlModel(**result)
        assert len(result['model_params'].keys()) > 0
        assert result['model_performance']['MSE'] > 0
        assert len(ml_result.model_name) > 0


@pytest.mark.simulate
@pytest.mark.celerylocal
def test_simulate_model(simulationmongo):
    model_name_list = [
        {
            'model_name': 'test_randomforest_normal20201101224257.joblib',
            'report_type': NORMAL_FINANCIAL_STATEMENTS,
            'test_code_list': [
                '2200',
            ]
        },
        {
            'model_name': 'test_randomforest_connected20201101224252.joblib',
            'report_type': CONNECTED_FINANCIAL_STATEMENTS,
            'test_code_list': [
                '2200',
            ]
        }
    ]
    for idx, m in enumerate(model_name_list):
        ml_result = MlModel(**m)
        ml_result.save(simulationmongo.ml_model_result)
        _ = simulate_model_result(
            db_name=settings.MONGODB_NAME,
            model_name=ml_result.model_name
        )
    result_list = list(
        simulationmongo.simulation_result.find(
           {}, {"_id": False}
        )
    )
    assert len(result_list) == 2
    for result in result_list:
        sldata = Simulation(
            **result
        )
        performance = sldata.strategy_performance
        assert performance['final_return'] >= 0
