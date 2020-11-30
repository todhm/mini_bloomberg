from datetime import timedelta, datetime as dt
import fp_types
import pytest
from tests.test_app import client
from dartapp.forms import ReportDict



@pytest.mark.apitest
def test_link_fetch_api():
    post_data = {
        'company':  '삼성전자',
        'code':  5930,
    }
    response = client.post(
        '/links', 
        json=post_data
    )
    result = response.json()
    print(len(result))
    
    assert(len(result) >= 80)
    period_type_list = [x['period_type'] for x in result]
    assert(fp_types.SEMINUAL_REPORT in period_type_list)
    assert(fp_types.SEPTEMBER_REPORT in period_type_list)
    assert(fp_types.MARCH_REPORT in period_type_list)
    assert(fp_types.YEARLY_REPORT in period_type_list)


@pytest.mark.apitest
@pytest.mark.apistartdate
def test_link_fetch_api_with_startdate():
    start_date = dt.strftime(dt.now() - timedelta(days=365), '%Y%m%d')
    post_data = {
        'company':  '삼성전자',
        'code':  5930,
        'start_date': start_date
    }
    response = client.post(
        '/links', 
        json=post_data
    )
    result = response.json()
    print(len(result))
    
    assert(len(result) > 0)
    assert(len(result) < 10)
    period_type_list = [x['period_type'] for x in result]
    assert(fp_types.SEMINUAL_REPORT in period_type_list)
    assert(fp_types.SEPTEMBER_REPORT in period_type_list)
    assert(fp_types.MARCH_REPORT in period_type_list)
    assert(fp_types.YEARLY_REPORT in period_type_list)


@pytest.mark.api
@pytest.mark.errorcase
def test_link_crawl_report():
    post_data = [{
        'code': '95700',
        'link': 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20110829000751',
        'reg_date': '2020-03-30',
        'corp_name': '제넥신',
        'market_type': '코스닥시장',
        'title': '사업보고서 (2019.12)',
        'period_type': '사업보고서',
        'reporter': '제넥신'
    }]
    response = client.post(
        '/report', 
        json={'linkList': post_data}
    )
    result = response.json()
    success_list = result.get('success_list')
    assert(len(success_list) == 2)
    for report in success_list:
        if report['report_type'] == fp_types.NORMAL_FINANCIAL_STATEMENTS:
            assert(report['current_assets'] == 6217838228054)
        else:
            assert(report['current_assets'] == 6762444473166)


@pytest.mark.apitest
@pytest.mark.error
def test_link_fetch_links():
    post_data = {
        'company':  '삼성전자',
        'code':  2200,
    }
    response = client.post(
        '/links', 
        json=post_data
    )
    result = response.json()
    print(len(result))
    
    assert(len(result) >= 2)
    period_type_list = [x['period_type'] for x in result]
    assert(fp_types.SEMINUAL_REPORT in period_type_list)
    assert(fp_types.MARCH_REPORT in period_type_list)


@pytest.mark.api
@pytest.mark.error
def test_link_error_case():
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20070330002067'
    post_data = [{
        'code': '95700',
        'link': link,
        'reg_date': '2020-03-30',
        'corp_name': '제넥신',
        'market_type': '코스닥시장',
        'title': '사업보고서 (2019.12)',
        'period_type': '사업보고서',
        'reporter': '제넥신'
    }]
    response = client.post(
        '/report', 
        json={'linkList': post_data}
    )
    result = response.json()
    success_list = result.get('success_list')
    assert(len(success_list) == 1)
    for result in success_list:
        if result['report_type'] == fp_types.NORMAL_FINANCIAL_STATEMENTS:
            assert(result['current_assets'] == 167385585074)
    post_data[0]['link'] = (
        'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20060331002614'
    )
    response = client.post(
        '/report', 
        json={'linkList': post_data}
    )
    result = response.json()
    success_list = result.get('success_list')
    assert(len(success_list) == 1)
    for result in success_list:
        if result['report_type'] == fp_types.NORMAL_FINANCIAL_STATEMENTS:
            assert(result['current_assets'] == 150101054126)
            assert(result['total_assets'] == 969086824160)
            assert(result['longterm_debt'] == 290660625299)
            assert(result['current_debt'] == 201504003959)
            assert(result['sales'] == 742679017965.0)
            assert(result['operational_income'] == 26128739708)
            assert(result['net_income'] == 38009110666)
            assert(result['cashflow_from_operation'] == 35195994633)    


@pytest.mark.api
@pytest.mark.apitest
def test_link_report_crawl():
    post_data = [{
        'code': '95700',
        'link': 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20200330003014',
        'reg_date': '2020-03-30',
        'corp_name': '제넥신',
        'market_type': '코스닥시장',
        'title': '사업보고서 (2019.12)',
        'period_type': '사업보고서',
        'reporter': '제넥신'
    }]
    response = client.post(
        '/report', 
        json={'linkList': post_data}
    )
    result = response.json()
    success_list = result.get('success_list')
    assert(len(success_list) >= 1)
    for idx, result in enumerate(success_list):
        report = ReportDict(**result)
        assert report.code == post_data[idx]['code']
        assert report.report_link == post_data[idx]['link']
        assert report.corp_name == post_data[idx]['corp_name']
        assert report.market_type == post_data[idx]['market_type']
        assert report.title == post_data[idx]['title']
        assert report.period_type == post_data[idx]['period_type']
        

@pytest.mark.api
@pytest.mark.apitest
def test_looking_minus_case():
    post_data = [{
        'code': '95700',
        'link': 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20030331001679',
        'reg_date': '2020-03-30',
        'corp_name': '제넥신',
        'market_type': '코스닥시장',
        'title': '사업보고서 (2019.12)',
        'period_type': '사업보고서',
        'reporter': '제넥신'
    }]
    response = client.post(
        '/report', 
        json={'linkList': post_data}
    )
    result = response.json()
    success_list = result.get('success_list')
    assert(len(success_list) == 2)
    for idx, result in enumerate(success_list):
        report = ReportDict(**result)
        assert report.code == post_data[0]['code']
        assert report.report_link == post_data[0]['link']
        assert report.corp_name == post_data[0]['corp_name']
        assert report.market_type == post_data[0]['market_type']
        assert report.title == post_data[0]['title']
        assert report.period_type == post_data[0]['period_type']
        if report.report_type == fp_types.NORMAL_FINANCIAL_STATEMENTS:
            assert report.book_value == 3515827104873
            assert report.current_assets == 1709718127782
            assert report.total_assets == 10497866854776
            assert report.longterm_debt == 4466086926736
            assert report.net_income == 111897096307
            assert report.sales == 6249700472132
            assert report.operational_income == 295151257697