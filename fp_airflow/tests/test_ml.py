import pytest
from config import LongRunningTestSettings
from dataclass_models.models import (
    TaskArgumentsList,
    MlModel
)
from task_functions import ml_models_tasks
import fp_types


@pytest.mark.ml
def test_save_ml_models(longrunningdb, execution_date):
    ml_models_tasks.create_machine_learning_models(
        db_name=LongRunningTestSettings.MONGODB_NAME, 
        execution_date=execution_date,
    )
    ts = execution_date.timestamp()
    ts = int(ts)
    data_list = TaskArgumentsList.fetch_total_arguments(
        longrunningdb,
        taskName=fp_types.SIMULATE_WITH_ML_MODELS,
        timestamp=ts,
    )
    assert len(data_list) == 2
    for data in data_list:
        mldata = MlModel(**data)
        assert len(data['model_params'].keys()) > 0
        assert data['model_performance']['MSE'] > 0
        assert len(mldata.model_name) > 0
        model_result = longrunningdb.ml_model_result.find_one({
            "model_name": mldata.model_name
        })
        assert model_result is not None

