
from config import TestSettings
from dataclass_models.models import CompanyReport
from task_functions import handle_dart_jobs
import pytest


@pytest.mark.report
@pytest.mark.reportlinks
def test_prepare_company_list(mongodb, execution_date):
    handle_dart_jobs.prepare_company_report_list(
        db_name=TestSettings.MONGODB_NAME
    )
    data_list = list(mongodb.company_list.find())
    original_length = len(data_list)
    handle_dart_jobs.prepare_company_report_list(
        db_name=TestSettings.MONGODB_NAME
    )
    new_length = mongodb.company_list.count()
    assert (original_length > 2000)
    assert (original_length == new_length)


@pytest.mark.report
@pytest.mark.reportlinks
def test_prepare_report_list(mongodb, execution_date):
    request_list = [
        {
            'company': '제넥신', 
            'code': 95700, 
            'company_category': '자연과학 및 공학 연구개발업', 
            'main_products': '항체융합단백질 치료제 및 유전자치료 백신개발', 
            'register_date': '20090915', 
            'accounting_month': '12월', 
            'ceo_name': '성영철', 
            'homepage': 'http://genexine.com', 
            'region': '경기도'
        }, 
        {
            'company': '디에스티', 
            'code': 33430, 'company_category': '특수 목적용 기계 제조업', 
            'main_products': '몰리브덴 등', 'register_date': '19980525', 
            'accounting_month': '12월', 
            'ceo_name': '김윤기, 양성문(각자 대표이사)', 
            'homepage': 'http://www.korid.co.kr', 
            'region': '경상남도'
        }
    ]
    data_list = [CompanyReport(**x).to_json for x in request_list]
    mongodb.company_list.insert_many(data_list)
    handle_dart_jobs.prepare_company_links(
        db_name=TestSettings.MONGODB_NAME, 
        execution_date=execution_date,
    )
    company_list = list(mongodb.company_list.find())
    assert len(company_list) == len(request_list)
    for data in company_list:
        assert len(data['link_list']) >= 10
    
    
@pytest.mark.reportapi
@pytest.mark.report
def test_reinsert_test(mongodb, execution_date):
    request_list = [
        {
            'company': '제넥신', 
            'code': 95700, 
            'company_category': '자연과학 및 공학 연구개발업', 
            'main_products': '항체융합단백질 치료제 및 유전자치료 백신개발', 
            'register_date': '20090915', 
            'accounting_month': '12월', 
            'ceo_name': '성영철', 
            'homepage': 'http://genexine.com', 
            'region': '경기도',
            "link_list": [
                {
                    'code': '95700',
                    'link': 'http://dart.fss.or.kr/dsaf001'
                        '/main.do?rcpNo=20200330003014',
                    'reg_date': '2020-03-30',
                    'corp_name': '제넥신',
                    'market_type': '코스닥시장',
                    'title': '사업보고서 (2019.12)',
                    'period_type': '사업보고서',
                    'reporter': '제넥신'
                }
            ]
        }, 
    ]
    data_list = [CompanyReport(**x).to_json for x in request_list]
    mongodb.company_list.insert_many(data_list)
    handle_dart_jobs.insert_company_data_list(
        db_name=TestSettings.MONGODB_NAME, 
        execution_date=execution_date,
    )
    report_counts = mongodb.report_data_list.count()
    assert(report_counts > 0)
    for request_data in request_list:
        data_list = list(
            mongodb.report_data_list.find(
                {'code': str(request_data['code'])}, {'code': 1}
            )
        )
        print(request_data['code'])
        assert(len(data_list) == report_counts)