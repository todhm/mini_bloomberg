from datetime import datetime as dt
import pytest
import requests 
import string
import random
import os
from darthandler import dartdataparsehandler
from darthandler.dartdatahandler import DartDataHandler
from darthandler.dartexcelparsinghandler import DartExcelParser
from darthandler import dartdriverparsinghandler
from darthandler.dartreporthandler import DartReportHandler
from fp_types import (
    YEARLY_REPORT, 
    QUARTER_REPORT, 
    MARCH_REPORT,
    SEPTEMBER_REPORT,
    SEMINUAL_REPORT
)
from utils.api_utils import return_sync_get_soup


@pytest.fixture(scope="module")
def ddh():
    # ...
    # 브라우저를 받아오는 부분
    # driver는 webDriver이다.
    ddh = DartDataHandler('testdb')
    yield ddh


def check_returned_report_link_data(data_list):
    for data in data_list:
        assert 'http' in data['link']
        assert data['code'].isdigit()
        assert data['market_type']
        assert data['market_type']
        assert type(data['reg_date']) is str
        date = dt.strptime(data['reg_date'], '%Y-%m-%d')
        assert type(date) is dt


@pytest.mark.linktest
def test_return_report_link_list(ddh):
    stock_code = 5930
    company_name = "삼성전자"
    report_list = ddh.return_company_report_link_list(
        stock_code, 
        company_name,
        QUARTER_REPORT
    )
    assert len(report_list) >= 30
    for report in report_list:
        assert type(report['link']) is str
        assert (
            type(
                dt.strptime(
                    report['reg_date'],
                    '%Y-%m-%d'
                )
            ) is
            dt
        )
        if '03' in report['title']:
            assert report['period_type'] == MARCH_REPORT
        elif '09' in report['title']:
            assert report['period_type'] == SEPTEMBER_REPORT
        assert type(report['market_type']) is str
        assert type(report['title']) is str
        assert type(report['corp_name']) is str


@pytest.mark.linktest
def test_return_seminual_report_link_list(ddh):
    stock_code = 5930
    company_name = "삼성전자"
    report_list = ddh.return_company_report_link_list(
        stock_code, 
        company_name,
        SEMINUAL_REPORT
    )
    assert (len(report_list) >= 15)
    for report in report_list:
        assert (type(report['link']) is str)
        assert(
            type(
                dt.strptime(
                    report['reg_date'],
                    '%Y-%m-%d'
                )
            ) is 
            dt
        )
        assert report['period_type'] == SEMINUAL_REPORT
        assert(type(report['market_type']) is str)
        assert(type(report['title']) is str)
        assert(type(report['corp_name']) is str)


@pytest.mark.linktest
def test_return_report_link_list_hyundai(ddh):
    stock_code = 5380
    company_name = "현대자동차"
    report_list = ddh.return_company_report_link_list(
        stock_code,
        company_name,
        YEARLY_REPORT
    )
    assert(len(report_list) >= 11)
    assert(type(report_list[0]['link']) is str)
    assert(type(dt.strptime(report_list[0]['reg_date'], '%Y-%m-%d')) is dt)
    assert(type(report_list[0]['market_type']) is str)
    assert(type(report_list[0]['title']) is str)
    assert(type(report_list[0]['corp_name']) is str)


@pytest.mark.asyncio()
async def test_excel_data_case(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190401003281'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 2)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            assert(result['current_assets'] == 31246722206)
            assert(
                result['cashflow_from_operation'] == 
                18368166497
            )    
            assert(result['total_assets'] == 91648911057)
            assert(result['longterm_debt'] == 2095096477)
            assert(result['current_debt'] == 23870127281)
            assert(result['book_value'] == 65683687299)
            assert(result['sales'] == 75944381164)
            assert(result['operational_income'] == 8476299586)
            assert(result['net_income'] == 6524582683)
        else:
            assert(result['current_assets'] == 45900138047)
            assert(
                result['cashflow_from_operation'] ==
                26996888606
            )
            assert(result['total_assets'] == 166309402053)
            assert(result['longterm_debt'] == 11068559688)
            assert(result['current_debt'] == 65993597856)
            assert(result['book_value'] == 89247244509)
            assert(result['sales'] == 152690561553)
            assert(result['operational_income'] == 15774064694)
            assert(result['net_income'] == 10166296071)


def test_return_finance_link_report_list(ddh):
    code = 311060
    date = '2018-12-28'
    reg_date = dt.strptime(date, "%Y-%m-%d")
    corp_name = '엘에이티'
    result = ddh.return_company_report_list(code, corp_name, reg_date)
    assert(len(result) >= 1)
    check_returned_report_link_data(result)


def test_parse_eq_offer_list(ddh):
    stock_code = 5930
    company_name = "삼성전자"
    eq_offer_list = ddh.return_company_eq_offer_lists(
        stock_code,
        company_name
    )
    assert(len(eq_offer_list) == 1)
    assert('reg_date' in eq_offer_list[0])
    

def test_eq_offer_api(ddh):
    stock_code = 5930
    company_name = "삼성전자"
    eq_offer_list = ddh.return_company_eq_offer_lists(stock_code, company_name)
    assert(len(eq_offer_list) == 1)
    assert('reg_date' in eq_offer_list[0])


@pytest.mark.asyncio()
async def test_statestment_with_noteincolumns(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190401004844'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 1)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            assert(result['current_assets'] == 12898820863)
            assert(result['cashflow_from_operation'] == -3134053492)    
            assert(result['total_assets'] == 24371975092)
            assert(result['longterm_debt'] == 5109253256)
            assert(result['current_debt'] == 3806369213)
            assert(result['book_value'] == 15456352623)
            assert(result['sales'] == 5448790067)
            assert(result['operational_income'] == -4603321664)
            assert(result['net_income'] == -40915428995)


@pytest.mark.asyncio()
async def test_where_multiple_columns_exists(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20030331001340'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 1)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            assert(result['current_assets'] == 2269897851)
            assert(result['cashflow_from_operation'] == -1315509824)    
            assert(result['total_assets'] == 5269596317)
            assert(result['longterm_debt'] == 2482274729)
            assert(result['current_debt'] == 899052938)
            assert(result['sales'] == 1174265504)
            assert(result['operational_income'] == -1128036308)
            assert(result['net_income'] == -1248898992)

    # self.check_two_period_data(link)
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20180402000209'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 1)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            assert(result['current_assets'] == 26670483762)
            assert(result['last_year_current_assets'] == 19854422081)    
            assert(result['last_year_current_debt'] == 29744559545)    
            assert(result['total_assets'] == 43378342172)
            assert(result['longterm_debt'] == 2428462140)
            assert(result['current_debt'] == 27600609347)
            assert(result['sales'] == 70377553599)
            assert(result['operational_income'] == 6386674207)
            assert(result['net_income'] == 7307898456)
            assert(result['cashflow_from_operation'] == 10952979075)    


@pytest.mark.asyncio()
async def test_where_multiple_title_column(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20001115000043'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 1)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            assert(result['current_assets'] == 3149876688)
            assert(result['total_assets'] == 3894849693)
            assert(result['longterm_debt'] == 1000000000)
            assert(result['current_debt'] == 502720137)
            assert(result['sales'] == 6129323104)
            assert(result['net_income'] == -524010221)


def test_driver_parsing_table(ddh):
    table_url = (
        'http://dart.fss.or.kr/report/viewer.do?'
        'rcpNo=20030407000694&dcmNo=582896&eleId=13134'
        '&offset=1542891&length=37875&dtd=dart2.dtd'
    )
    soup = return_sync_get_soup(table_url)
    financial_table_data = (
        dartdataparsehandler
        .return_financial_report_table(
            table_url,
            soup
        )
    )
    result = dartdriverparsinghandler.return_driver_report_data(
        table_url, financial_table_data, {}
    )
    expected_current_assets = 12079994
    cashflow_from_operation = 11193197
    total_assets = 34439600
    longterm_debt = 1710645
    current_debt = 8418665
    sales = 40511563
    net_income = 7051761
    operational_income = 7244672
    assert(
        result['current_assets'] == 
        expected_current_assets * 1000000
    )
    assert(
        result['cashflow_from_operation'] == 
        cashflow_from_operation * 1000000
    )    
    assert(result['total_assets'] == total_assets * 1000000)
    assert(result['longterm_debt'] == longterm_debt * 1000000)
    assert(result['current_debt'] == current_debt * 1000000)
    assert(result['sales'] == sales * 1000000)
    assert(result['operational_income'] == operational_income * 1000000)
    assert(result['net_income'] == net_income * 1000000)


@pytest.mark.asyncio()
async def test_driver_parsing_list_case(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20030407000694'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 2)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            expected_current_assets = 12079994
            cashflow_from_operation = 11193197
            total_assets = 34439600
            longterm_debt = 1710645
            current_debt = 8418665
            total_book_value = 24310290
            sales = 40511563
            net_income = 7051761
            operational_income = 7244672
            assert(
                result['current_assets'] == expected_current_assets * 1000000
            )
            assert(
                result['cashflow_from_operation'] == 
                cashflow_from_operation * 1000000
            )    
            assert(result['total_assets'] == total_assets*1000000)
            assert(result['longterm_debt'] == longterm_debt*1000000)
            assert(result['current_debt'] == current_debt*1000000)
            assert(result['book_value'] == total_book_value*1000000)
            assert(result['sales'] == sales * 1000000)
            assert(
                result['operational_income'] == 
                operational_income * 1000000
            )
            assert(result['net_income'] == net_income * 1000000)

        else:
            expected_current_assets = 26870721
            cashflow_from_operation = 4645165
            total_book_value = 20398519
            total_assets = 52114878
            longterm_debt = 8154740
            current_debt = 23561619
            sales = 46443768
            net_income = 3370912
            operational_income = 3951428
            assert(
                result['current_assets'] ==
                expected_current_assets * 1000000
            )
            assert(
                result['cashflow_from_operation'] == 
                cashflow_from_operation * 1000000
            )    
            assert(result['total_assets'] == total_assets * 1000000)
            assert(result['longterm_debt'] == longterm_debt * 1000000)
            assert(result['current_debt'] == current_debt * 1000000)
            assert(result['book_value'] == total_book_value * 1000000)
            assert(result['sales'] == sales * 1000000)
            assert(
                result['operational_income'] == 
                operational_income * 1000000
            )
            assert(result['net_income'] == net_income * 1000000)


@pytest.mark.asyncio()
async def test_samsung_error_data_parsign(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20000330000796'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 2)
    for result in result_list:
        if result.get('report_type') == 'NORMAL_FINANCIAL_STATEMENTS':
            assert(result['current_assets'] == 6197231279000)
            assert(result['total_assets'] == 24709802882000)
            assert(result['longterm_debt'] == 4597362974000)
            assert(result['current_debt'] == 6780871440000)
            assert(result['sales'] == 26117785751000)
            assert(result['operational_income'] == 4481500117000)
            assert(result['net_income'] == 3170402574000)
            assert(result['cashflow_from_operation'] == 7077732141000)  

        else:
            assert(result['current_assets'] == 9720154667000)
            assert(result['total_assets'] == 24104678550000)
            assert(result['longterm_debt'] == 9667050861000)
            assert(result['current_debt'] == 9349147376000)
            assert(result['sales'] == 25772311281000.0)
            assert(result['operational_income'] == 2866238549000)
            assert(result['net_income'] == -362253077000)
            assert(result['cashflow_from_operation'] == 6077767311000)  

        
@pytest.mark.asyncio()
async def test_table_data_parsing(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190515000764'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 2)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            expected_current_assets = 34344697663
            cashflow_from_operation = 4987111141
            total_assets = 96806051646
            longterm_debt = 2658019397
            current_debt = 28318511450
            sales = 20839467514
            net_income = 3326998070
            operational_income = 3316076095
            assert(result['current_assets'] == expected_current_assets)
            assert(
                result['cashflow_from_operation'] == cashflow_from_operation
            )    
            assert(result['total_assets'] == total_assets)
            assert(result['longterm_debt'] == longterm_debt)
            assert(result['current_debt'] == current_debt)
            assert(result['sales'] == sales)
            assert(result['operational_income'] == operational_income)
            assert(result['net_income'] == net_income)

        else:
            expected_current_assets = 48847498882
            cashflow_from_operation = 12460770632
            total_assets = 173536441461
            longterm_debt = 12384792007
            current_debt = 70635438719
            sales = 44091600036
            net_income = 4843725571
            operational_income = 6709454095
            assert(result['current_assets'] == expected_current_assets)
            assert(
                result['cashflow_from_operation'] == cashflow_from_operation
            )    
            assert(result['total_assets'] == total_assets)
            assert(result['longterm_debt'] == longterm_debt)
            assert(result['current_debt'] == current_debt)
            assert(result['sales'] == sales)
            assert(result['operational_income'] == operational_income)
            assert(result['net_income'] == net_income)


@pytest.mark.asyncio()
async def test_korean_air_report(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20160516003079'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 2)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            assert(result['current_assets'] == 2873562672777)
            assert(result['cashflow_from_operation'] == 2666464423603)
            assert(result['total_assets'] == 23048939945281)
            assert(result['longterm_debt'] == 12324531908724)
            assert(result['current_debt'] == 8428114074534)
            assert(result['sales'] == 11308423372686)
            assert(result['operational_income'] == 859217822453)
            assert(result['net_income'] == -407682308362)

        else:
            assert(result['current_assets'] == 3289127053478)
            assert(result['cashflow_from_operation'] == 2728023077614)    
            assert(result['total_assets'] == 24180351112715)
            assert(result['longterm_debt'] == 13230934646401)
            assert(result['current_debt'] == 8450381325032)
            assert(result['sales'] == 11544831301113)
            assert(result['operational_income'] == 883088280640)
            assert(result['net_income'] == -562967287220)


@pytest.mark.asyncio()
async def test_report_cases(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20160516003079'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 2)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            expected_current_assets = 2873562672777
            cashflow_from_operation = 2666464423603
            total_assets = 23048939945281
            longterm_debt = 12324531908724
            current_debt = 8428114074534
            sales = 11308423372686
            net_income = -407682308362
            operational_income = 859217822453
            assert(result['current_assets'] == expected_current_assets)
            assert(
                result['cashflow_from_operation'] == cashflow_from_operation
            )    
            assert(result['total_assets'] == total_assets)
            assert(result['longterm_debt'] == longterm_debt)
            assert(result['current_debt'] == current_debt)
            assert(result['sales'] == sales)
            assert(result['operational_income'] == operational_income)
            assert(result['net_income'] == net_income)

        else:
            expected_current_assets = 174697424
            cashflow_from_operation = 67031863
            total_assets = 339357244
            longterm_debt = 22522557
            current_debt = 69081510
            sales = 243771415
            net_income = -562967287220
            operational_income = 883088280640
            assert(result['current_assets'] == 3289127053478)
            assert(result['cashflow_from_operation'] == 2728023077614)    
            assert(result['total_assets'] == 24180351112715)
            assert(result['longterm_debt'] == 13230934646401)
            assert(result['current_debt'] == 8450381325032)
            assert(result['sales'] == 11544831301113)
            assert(result['operational_income'] == 883088280640)
            assert(result['net_income'] == -562967287220)


@pytest.mark.asyncio()
async def test_excel_data_parsing(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190401004781'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 2)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            expected_current_assets = 80039455
            cashflow_from_operation = 44341217
            total_assets = 219021357
            longterm_debt = 2888179
            current_debt = 43145053
            sales = 170381870
            net_income = 32815127
            operational_income = 43699451
            assert(
                result['current_assets'] == expected_current_assets*1000000
            )
            assert(
                result['cashflow_from_operation'] == 
                cashflow_from_operation*1000000
            )    
            assert(result['total_assets'] == total_assets*1000000)
            assert(result['longterm_debt'] == longterm_debt*1000000)
            assert(result['current_debt'] == current_debt*1000000)
            assert(result['sales'] == sales*1000000)
            assert(result['operational_income'] == operational_income*1000000)
            assert(result['net_income'] == net_income*1000000)

        else:
            expected_current_assets = 174697424
            cashflow_from_operation = 67031863
            total_assets = 339357244
            longterm_debt = 22522557
            current_debt = 69081510
            sales = 243771415
            net_income = 44344857
            operational_income = 58886669
            assert(result['current_assets'] == expected_current_assets*1000000)
            assert(
                result['cashflow_from_operation'] == 
                cashflow_from_operation*1000000
            )    
            assert(result['total_assets'] == total_assets*1000000)
            assert(result['longterm_debt'] == longterm_debt*1000000)
            assert(result['current_debt'] == current_debt*1000000)
            assert(result['sales'] == sales*1000000)
            assert(result['operational_income'] == operational_income*1000000)
            assert(result['net_income'] == net_income*1000000)


@pytest.mark.asyncio()
async def test_error_case_laontech(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20200330002340'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 1)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            expected_current_assets = 7735651632
            cashflow_from_operation = -285756285
            total_assets = 18754930175
            assert(result['current_assets'] == expected_current_assets)
            assert(
                result['cashflow_from_operation'] 
                == cashflow_from_operation
            )    
            assert(result['total_assets'] == total_assets)
            assert(result['longterm_debt'] == 9673179122)
            assert(result['current_debt'] == 5784704873)
            assert(result['sales'] == 12632423500)
            assert(result['operational_income'] == -1211469053)
            assert(result['net_income'] == -1431915408)
            assert(result['book_value'] == 3297046180)


@pytest.mark.asyncio()
async def test_laontech_second_error_case(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190401002805'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 1)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            expected_current_assets = 12074416412
            assert(result['current_assets'] == expected_current_assets)


@pytest.mark.asyncio()            
async def test_laontech_third_error_case(ddh): 
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20151215000033'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 1)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            expected_current_assets = 3767375000
            assert(result['current_assets'] == expected_current_assets)
            assert(result['cashflow_from_operation'] == -361962000)
            assert(result['net_income'] == 539589000)


@pytest.mark.asyncio()
async def test_hanzin_first_error_case(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20150331003586'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 2)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            assert(result['current_assets'] == 24374000000)
            assert(result['cashflow_from_operation'] == 9367000000)    
            assert(result['total_assets'] == 1124296000000)
            assert(result['longterm_debt'] == 34514000000)
            assert(result['current_debt'] == 30904000000)
            assert(result['sales'] == 15091000000)
            assert(result['operational_income'] == 11061000000)
            assert(result['net_income'] == 9092000000)
            assert(result['book_value'] == 1058878000000)
        else:
            assert(result['current_assets'] == 456871000000)
            assert(result['cashflow_from_operation'] == 50230000000)    
            assert(result['total_assets'] == 2722203000000)
            assert(result['longterm_debt'] == 1044949000000)
            assert(result['current_debt'] == 569946000000)
            assert(result['sales'] == 1520058000000)
            assert(result['operational_income'] == -79709000000)
            assert(result['net_income'] == -135021000000)
            assert(result['book_value'] == 1107308000000)


@pytest.mark.asyncio()
async def test_hanzin_tech_error_case(ddh):
    link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20040510001670'
    result_list, failed_result = await ddh.return_reportlink_data(link=link)
    assert(len(result_list) == 1)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            assert(result['current_assets'] == 2018848156000)
            assert(result['cashflow_from_operation'] == 847485458000)    
            assert(result['total_assets'] == 3547476247000)
            assert(result['longterm_debt'] == 1448125545000)
            assert(result['current_debt'] == 1048617907000)
            assert(result['sales'] == 1543948233000)
            assert(result['operational_income'] == 85408690000)
            assert(result['net_income'] == 24758249000)
            assert(result['book_value'] == 1050732795000)


@pytest.mark.asyncio()
async def test_shinhan_report_parsing(ddh):
    url = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20121026000269'
    result_list, failed_result = await ddh.return_reportlink_data(link=url)
    assert(len(result_list) == 2)
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            assert(result['cashflow_from_operation'] == 1343507000000)    
            assert(result['total_assets'] == 30844250000000)
            assert(result['operational_income'] == 1679986000000)
            assert(result['net_income'] == 1672908000000)
            assert(result['book_value'] == 19430807000000)
        else:
            assert(result['cashflow_from_operation'] == 1343507000000)    
            assert(result['total_assets'] == 288041796000000)
            assert(result['operational_income'] == 4134772000000)
            assert(result['net_income'] == 3272633000000)
            assert(result['book_value'] == 26858805000000)


@pytest.mark.asyncio()
async def test_failed_flower_firms(ddh):
    url = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20100503001219'
    result_list, failed_result = await ddh.return_reportlink_data(link=url)
    assert(len(result_list) == 1)
    result = result_list[0]
    assert(result['current_assets'] == 350604075195)
    assert(result['cashflow_from_operation'] == 127791674884)    
    assert(result['total_assets'] == 751085360770)
    assert(result['longterm_debt'] == 45977870766)
    assert(result['current_debt'] == 205753622377)
    assert(result['sales'] == 718165366951)
    assert(result['operational_income'] == 64264962312)
    assert(result['net_income'] == 54845066144)
    assert(result['book_value'] == 499353867627)


def test_excel_parsing_error_case(ddh):
    url = (
        'http://dart.fss.or.kr/pdf/download/excel.do'
        '?rcp_no=20150331003586&dcm_no=4559882&lang=ko'
    )
    random_file_name = ''.join(
        random.choice(string.ascii_uppercase + string.digits) 
        for _ in range(10)
    ) + '.xls'
    user_agent = (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) '
        'AppleWebKit/602.2.14 (KHTML, like Gecko) '
        'Version/10.0.1 Safari/602.2.14'
    )
    headers = {
            'User-Agent': user_agent,
            'Accept': (
                'text/html,application/xhtml+xml,'
                'application/xml;q=0.9,image/webp,*/*;q=0.8'
            ),
            'Connection': 'keep-alive'
    }
    with open('./datacollections/' + random_file_name, 'wb') as f:
        response = requests.get(url, allow_redirects=True, headers=headers)
        content = response.content
        f.write(content)
    dep = DartExcelParser(
        link='testlink', 
        excel_link=url,
        fname='./datacollections/' + random_file_name,
        code='000', 
        corp_name='testcorp', 
        period_type='tesperiod',
        reg_date='testregdate',
        market_type='test', 
        reporter='test',
        title='test'
    )
    result_list = dep.parse_excel_file_data()
    assert len(result_list) == 2
    for result in result_list:
        if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
            assert(result['current_assets'] == 2873562672777)
            assert(result['cashflow_from_operation'] == 2666464423603)
            assert(result['total_assets'] == 23048939945281)
            assert(result['longterm_debt'] == 12324531908724)
            assert(result['current_debt'] == 8428114074534)
            assert(result['sales'] == 11308423372686)
            assert(result['operational_income'] == 859217822453)
            assert(result['net_income'] == -407682308362)

        else:
            assert(result['current_assets'] == 3289127053478)
            assert(result['cashflow_from_operation'] == 2728023077614)    
            assert(result['total_assets'] == 24180351112715)
            assert(result['longterm_debt'] == 13230934646401)
            assert(result['current_debt'] == 8450381325032)
            assert(result['sales'] == 11544831301113)
            assert(result['operational_income'] == 883088280640)
            assert(result['net_income'] == -562967287220)
    os.remove('./datacollections/' + random_file_name)


def test_samsung_normal_parse_error(ddh):
    table_url = (
        'http://dart.fss.or.kr/report/viewer.do'
        '?rcpNo=20000330000796&dcmNo=41243&eleId=7334'
        '&offset=865570&length=405051&dtd=dart2.dtd'
    )
    soup = return_sync_get_soup(table_url)
    drh = DartReportHandler(
        link='link',
        table_link=table_url,
        soup=soup,
        code='testcode', 
        corp_name='testcompany',
        report_type='testreport',
        period_type='testperiods',
        reg_date='testreg', 
        market_type='testtype'
    )
    result = drh.parse_report_link()
    assert(result['current_assets'] == 9720154667000)
    assert(result['total_assets'] == 24104678550000)
    assert(result['longterm_debt'] == 9667050861000)
    assert(result['current_debt'] == 9349147376000)
    assert(result['sales'] == 25772311281000.0)
    assert(result['operational_income'] == 2866238549000)
    assert(result['net_income'] == -362253077000)
    assert(result['cashflow_from_operation'] == 6077767311000)    


@pytest.mark.asyncio()
@pytest.mark.driver
async def test_driver_error_case(ddh):
    url = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20030813000292'
    result_list, failed_result = await ddh.return_reportlink_data(link=url)
    assert(len(result_list) == 1)
    result = result_list[0]
    assert(result['current_assets'] == 37699490881)
    assert(result['cashflow_from_operation'] == 4425941755)    
    assert(result['total_assets'] == 160800914775)
    assert(result['longterm_debt'] == 9747458927)
    assert(result['current_debt'] == 36526514384)
    assert(result['sales'] == 26151312347)
    assert(result['operational_income'] == 1050599514)
    assert(result['net_income'] == 723505382)
    assert(result['book_value'] == 114526941464)


@pytest.mark.asyncio()
@pytest.mark.errorcase
async def test_report_error(ddh):
    url = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20130304000404'
    result_list, failed_result = await ddh.return_reportlink_data(link=url)
    assert(len(result_list) == 1)
    result = result_list[0]
    assert(result['current_assets'] == 37699490881)
    assert(result['cashflow_from_operation'] == 4425941755)    
    assert(result['total_assets'] == 160800914775)
    assert(result['longterm_debt'] == 9747458927)
    assert(result['current_debt'] == 36526514384)
    assert(result['sales'] == 26151312347)
    assert(result['operational_income'] == 1050599514)
    assert(result['net_income'] == 723505382)
    assert(result['book_value'] == 114526941464)
