import pytest
from tests.lr_test_app import settings
from models.dataclass_models import MlModel
from ml_models.tasks import save_ml_models


@pytest.mark.mlmodel
@pytest.mark.celerylocal
def test_save_ml_models(longrunningmongo):
    result = save_ml_models(
        db_name=settings.MONGODB_NAME,
        model_name='test_randomforest'
    )
    assert len(result) == 2
    result_list = list(
        longrunningmongo.ml_model_result.find(
           {}, {"_id": False}
        )
    )
    assert len(result_list) == 2
    for result in result_list:
        ml_result = MlModel(**result)
        assert len(result['model_params'].keys()) > 0
        assert result['model_performance']['MSE'] > 0
        assert len(ml_result.model_name) > 0

    

