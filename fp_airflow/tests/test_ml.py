import pytest
from config import (
    SimulationTestSettings
)
from dataclass_models.models import (
    TaskArgumentsList,
    MlModel
)
from task_functions import ml_models_tasks
import fp_types


@pytest.mark.ml
@pytest.mark.ml_model
def test_save_ml_models(simulationdb, execution_date):
    ml_models_tasks.create_machine_learning_models(
        db_name=SimulationTestSettings.MONGODB_NAME, 
        execution_date=execution_date,
    )
    ts = execution_date.timestamp()
    ts = int(ts)
    data_list = TaskArgumentsList.fetch_total_arguments(
        simulationdb,
        taskName=fp_types.SIMULATE_WITH_ML_MODELS,
        timestamp=ts,
    )
    assert len(data_list) == 2
    for data in data_list:
        mldata = MlModel(**data)
        assert len(data['model_params'].keys()) > 0
        assert data['model_performance']['MSE'] > 0
        assert len(mldata.model_name) > 0
        model_result = simulationdb.ml_model_result.find_one({
            "model_name": mldata.model_name
        })
        assert model_result is not None


@pytest.mark.ml
@pytest.mark.simulation
def test_make_simulations(simulationdb, execution_date):
    test_model_list = [
        {
            'model_name': 'test_randomforest_normal20201101224257.joblib',
            'report_type': fp_types.NORMAL_FINANCIAL_STATEMENTS,
            'test_code_list': [
                '3490',
                '2200',
            ]
        },
        {
            'model_name': 'test_randomforest_connected20201101224252.joblib',
            'report_type': fp_types.CONNECTED_FINANCIAL_STATEMENTS,
            'test_code_list': [
                '3490',
                '2200',
            ]
        }
    ]
    for data in test_model_list:
        mldata = MlModel(**data)
        mldata.save(simulationdb.ml_model_result)
    ts = execution_date.timestamp()
    ts = int(ts)
    ta = TaskArgumentsList(
        timestamp=ts, 
        dataList=test_model_list, taskName=fp_types.SIMULATE_WITH_ML_MODELS
    )
    ta.save(simulationdb)
    ml_models_tasks.create_simulation_results(
        db_name=SimulationTestSettings.MONGODB_NAME, 
        execution_date=execution_date,
    )
    result_list = list(
        simulationdb.simulation_result.find(
           {}, {"_id": False}
        )
    )
    assert len(result_list) == 2
    