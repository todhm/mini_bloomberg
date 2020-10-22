import pytest
from tests.lr_test_app import settings, client
from celery import states
from pipeline.tasks import save_machinelearing_features_data


@pytest.mark.celerytasks
def test_company_pipeline_tasks(longrunningmongo):
    test_company_code = "2200"
    save_machinelearing_features_data(test_company_code, settings.MONGODB_NAME)
    feature_list = list(
        longrunningmongo.ml_feature_list.find({'code': test_company_code})
    )
    assert len(feature_list) >= 1000


@pytest.mark.celerytasks
def test_report_data_second_error_case(longrunningmongo):
    test_company_code = "2200"
    post_data = {
        'taskFunc': 'save_machinelearing_features_data',
        'data': {
            'db_name': settings.MONGODB_NAME, 
            'code': test_company_code
        }
    } 
    response = client.post(
        '/launch_celery_tasks', 
        json=post_data
    )
    result = response.json()
    print(result)
    celery_id = result['taskId']
    finished = False
    total_time = 0
    while not finished:
        total_time += 1
        response = client.get(
            f'/tasks/{celery_id}', 
            json=post_data
        )
        if response.json()['status'] == states.SUCCESS:
            break
    assert response.json()['status'] == states.SUCCESS
    feature_list = list(
        longrunningmongo.ml_feature_list.find({'code': test_company_code})
    )
    assert len(feature_list) >= 1000

