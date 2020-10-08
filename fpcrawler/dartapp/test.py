import json
import fp_types
import pytest
from tests.test_app import client


def test_report_data_api():
    post_data = {
        'company':  '삼성전자',
        'code':  5930,
        'start_date': '20160101'
    }
    response = client.post(
        '/company_report_data_list', 
        json=post_data
    )
    result = response.json()
    success_list = result.get('success_list')
    assert(len(success_list) >= 3)
    period_type_list = [x['period_type'] for x in success_list]
    assert(fp_types.SEMINUAL_REPORT in period_type_list)
    assert(fp_types.SEPTEMBER_REPORT in period_type_list)
    assert(fp_types.MARCH_REPORT in period_type_list)
    assert(fp_types.YEARLY_REPORT in period_type_list)


def test_report_data_api_error_case():
    post_data =   {'company': '디에스티', 'code': 33430, 'company_category': '특수 목적용 기계 제조업', 'main_products': '몰리브덴 등', 'register_date': '19980525', 'accounting_month': '12월', 'ceo_name': '김윤기, 양성문(각자 대표이사)', 'homepage': 'http://www.korid.co.kr', 'region': '경상남도', 'start_date': '20160101'}
    response = client.post(
        '/company_report_data_list', 
        json=post_data
    )
    result = response.json()
    success_list = result.get('success_list')
    assert(len(success_list) >= 1)


def test_report_api_third_case():
    post_data = {
            'company': '제넥신', 
            'code': 95700, 
            'company_category': '자연과학 및 공학 연구개발업', 
            'main_products': '항체융합단백질 치료제 및 유전자치료 백신개발', 
            'register_date': '20090915', 
            'accounting_month': '12월', 
            'ceo_name': '성영철', 
            'homepage': 'http://genexine.com', 
            'region': '경기도'
    }
    response = client.post(
        '/company_report_data_list', 
        json=post_data
    )
    result = json.loads(response.data)
    success_list = result.get('success_list')
    assert(len(success_list) >= 10)
    print(result.get('failed_list'))


def test_report_data_second_error_case():
    post_data = {
            'company': '상신전자', 
            'code': 263810, 'company_category': '전자부품 제조업', 
            'main_products': '노이즈필터(48.81%), 코일(19.33%), 리액터(10.75%)', 
            'register_date': '20171016', 'accounting_month': '12월', 
            'ceo_name': '김승천', 'homepage': 'http://www.sangshin-e.com', 
            'region': '경상남도'
    } 
    response = client.post(
        '/company_report_data_list', 
        json=post_data
    )
    result = response.json()
    success_list = result.get('success_list')
    assert(len(success_list) >= 1)


def test_report_data_api_with_quarter():
    post_data =  {
        'company': '엘에이티', 
        'code': 311060, 
        'company_category': '그외 기타 전문, 과학 및 기술 서비스업', 
        'main_products': '유전체 분석 서비스 (NGS, CES 등)', 
        'register_date': '20200713', 
        'accounting_month': '12월', 
        'ceo_name': '김운봉', 'homepage': '', 'region': '미국',
        'report_type': fp_types.QUARTER_REPORT,
        'start_date': '20160101'
    }
    response = client.post(
        '/company_report_data_list', 
        json=post_data
    )
    result = response.json()
    success_list = result.get('success_list')
    assert(len(success_list) >= 1)


def test_report_async_error_case():
    post_data = {
        'code': 25750, 
        'company': '한솔홈데코', 
        'company_category': '제재 및 목재 가공업', 
        'main_products': 'PB,MDF,바닥재,강화,재생목재,LPM,제재목 제조,도소매,무역/임업,벌목관련 사업', 
        'start_date': '20160101', 
        'register_date': '20031104'
    }
    response = client.post(
        '/company_report_data_list', 
        json=post_data
    )
    result = response.json()
    success_list = result.get('success_list')
    assert(len(success_list) >= 1)


def test_report_airflow_error_case():
    post_data = {
        'code': 50, 
        'company': '경방', 
        'company_category': '제재 및 목재 가공업', 
        'main_products': 'PB,MDF,바닥재,강화,재생목재,LPM,제재목 제조,도소매,무역/임업,벌목관련 사업', 
        'start_date': '20180101', 
        'register_date': '20031104'
    }
    response = client.post(
        '/company_report_data_list', 
        json=post_data
    )
    result = response.json()
    success_list = result.get('success_list')
    print(success_list)
    report_link_list = [x['report_link'] for x in success_list]
    period_type_list = [x['period_type'] for x in success_list]
    assert(len(success_list) >= 10)
    assert(len(set(report_link_list)) >= 10)
    assert(len(success_list) > len(result['failed_list']))
    for x in [
        fp_types.YEARLY_REPORT, 
        fp_types.SEMINUAL_REPORT, 
        fp_types.SEPTEMBER_REPORT, 
        fp_types.MARCH_REPORT
    ]:
        assert(x in period_type_list)


def test_new_airflow_case():
    post_data = {
        'code': 1130, 
        'company': '대한제분', 
        'company_category': '제재 및 목재 가공업', 
        'main_products': 'PB,MDF,바닥재,강화,재생목재,LPM,제재목 제조,도소매,무역/임업,벌목관련 사업', 
        'start_date': '20180101', 
        'register_date': '20031104'
    }
    response = client.post(
        '/company_report_data_list', 
        json=post_data
    )
    result = response.json()
    success_list = result.get('success_list')
    print(len(result['success_list']))
    print(result['failed_list'])
    report_link_list = [x['report_link'] for x in success_list]
    period_type_list = [x['period_type'] for x in success_list]
    report_type_list = [x['report_type'] for x in success_list]
    print(report_link_list)
    print(report_type_list)
    assert(len(success_list) >= 10)
    assert(len(set(report_link_list)) >= 10)
    assert(len(success_list) > len(result['failed_list']))
    for x in [
        fp_types.YEARLY_REPORT, 
        fp_types.SEMINUAL_REPORT, 
        fp_types.SEPTEMBER_REPORT, 
        fp_types.MARCH_REPORT
    ]:
        assert(x in period_type_list)

# def test_fund_data_list_api(self):
#     data_list = [
#         {
#             'first_level': '해외투자형', 
#             'second_level': '채권형', 
#             'third_level': '채권투자형-일반',
#             'year': 2010,
#             'month': 1,
#         },
#     ]
#     response = self.client.post(
#         '/return_fund_data_list', 
#         json={'dataList': data_list}
#     )
#     result = json.loads(response.data)
#     assert(len(result), 1)
#     self.assertTrue(len(result[0]) >= 200)
#     for x in result[0]:
#         assert(type(x['company']), str)
#         assert(type(x['위험등급']), str)
#         assert(type(x['class']), str)
#         assert(type(x['total_amount']), int)
#         assert(type(x['stock_amount']), int)
#         assert(type(x['stock_ratio'])== float)
#         assert(type(x['bond_amount']), int)
#         assert(type(x['bond_ratio'])== float)
#         assert(type(x['security_contract_amount']), int)
#         assert(type(x['real_estate_amount']), int)
#         assert(type(x['real_estate_ratio'])== float)
#         assert(type(x['goods_amount']), int)
#         assert(type(x['goods_ratio'])== float)
#         assert(type(x['cash_amount']), int)
#         assert(type(x['cash_ratio'])== float)
#         assert(type(x['standard_price'])== float)
#         assert(type(x['one_month_return'])== float)
#         self.assertTrue(
#             type(x['one_month_rank']) is int or x['one_month_rank'] is None
#         ) 
#         assert(type(x['six_month_return'])== float)
#         self.assertTrue(
#             type(x['six_month_rank']) is int or x['six_month_rank'] is None
#         ) 
#         assert(type(x['one_year_return'])== float)
#         self.assertTrue(
#             type(x['one_year_rank']) is int or x['one_year_rank'] is None
#         )
#         assert(type(x['end_period_return'])== float)
#         self.assertTrue(
#             type(x['end_period_rank']) is int or 
#             x['end_period_rank'] is None
#         )
#         assert(type(x['operational_cost'])== float)
#         assert(type(x['sales_cost'])== float)
#         assert(type(x['sales_comission'])== float)
#         assert(type(x['sales_company']), str)