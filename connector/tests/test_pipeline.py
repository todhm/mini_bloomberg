import pytest
from tests.lr_test_app import settings, client
from celery import states
from models.schema import MachineLearningSaveSchema
from pipeline.tasks import save_machinelearing_features_data
import fp_types


@pytest.mark.celerylocal
def test_company_pipeline_tasks(longrunningmongo):
    test_company_code = "2200"
    result = save_machinelearing_features_data(
        test_company_code, settings.MONGODB_NAME
    )
    assert(fp_types.NORMAL_FINANCIAL_STATEMENTS in result['success_list'])
    assert(fp_types.CONNECTED_FINANCIAL_STATEMENTS in result['success_list'])
    normal_feature_list = list(
        longrunningmongo.ml_feature_list.find(
            {
                'code': test_company_code,
                'report_type': fp_types.NORMAL_FINANCIAL_STATEMENTS
            },
            {
                "_id": False
            }
        )
    )
    connected_feature_list = list(
        longrunningmongo.ml_feature_list.find(
            {
                'code': test_company_code,
                'report_type': fp_types.CONNECTED_FINANCIAL_STATEMENTS
            },
            {
                "_id": False
            }
        )
    )
    schema = MachineLearningSaveSchema(many=True)
    _ = schema.load(normal_feature_list)
    _ = schema.load(connected_feature_list)
    dt_list = set([x['stock_date'] for x in normal_feature_list])
    connected_dt_list = set([x['stock_date'] for x in connected_feature_list])
    assert len(normal_feature_list) >= 1000
    assert len(connected_feature_list) >= 1000
    assert len(dt_list) == len(normal_feature_list)
    assert len(connected_dt_list) == len(connected_feature_list)


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
    celery_id = result['taskId']
    finished = False
    total_time = 0
    while not finished:
        total_time += 1
        response = client.get(
            f'/tasks/{celery_id}', 
            json=post_data
        )
        if response.json()['state'] == states.SUCCESS:
            break
    assert response.json()['state'] == states.SUCCESS
    result = response.json()['data']
    success_list = result['success_list']
    assert(
        fp_types.NORMAL_FINANCIAL_STATEMENTS
        in success_list
    )
    assert(
        fp_types.CONNECTED_FINANCIAL_STATEMENTS
        in success_list
    )
    normal_feature_list = list(
        longrunningmongo.ml_feature_list.find(
            {
                'code': test_company_code,
                'report_type': fp_types.CONNECTED_FINANCIAL_STATEMENTS
            }, 
            {"_id": False}
        )
    )
    connected_feature_list = list(
        longrunningmongo.ml_feature_list.find(
            {
                'code': test_company_code,
                'report_type': fp_types.CONNECTED_FINANCIAL_STATEMENTS
            }, 
            {"_id": False}
        )
    )
    schema = MachineLearningSaveSchema(many=True)
    _ = schema.load(normal_feature_list)
    _ = schema.load(connected_feature_list)
    dt_list = set([x['stock_date'] for x in normal_feature_list])
    connected_dt_list = set([x['stock_date'] for x in connected_feature_list])
    assert len(normal_feature_list) >= 1000
    assert len(connected_feature_list) >= 1000
    assert len(dt_list) == len(normal_feature_list)
    assert len(connected_dt_list) == len(connected_feature_list)