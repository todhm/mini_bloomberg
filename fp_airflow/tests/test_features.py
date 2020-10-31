import pytest
from config import LongRunningTestSettings
from dataclass_models.models import CompanyReport
from task_functions import create_feature_tasks


@pytest.mark.features
def test_save_market_crawling_dates(longrunningdb, execution_date):
    request_list = [
        {
            "code": "2200", 
            "corp_name": "한국수출포장공업"
        }
    ]
    data_list = [CompanyReport(**x).to_json for x in request_list]
    longrunningdb.company_list.insert_many(data_list)
    create_feature_tasks.create_machine_learning_features(
        db_name=LongRunningTestSettings.MONGODB_NAME, 
        execution_date=execution_date,
    )