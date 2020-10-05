from datetime import datetime as dt
import os 
import json
from random import randint
import pytest
from darthandler.dartdatahandler import DartDataHandler
from darthandler import dartdataparsehandler
from darthandler import dartdriverparsinghandler
from fp_types import (
    YEARLY_REPORT, 
    QUARTER_REPORT, 
    MARCH_REPORT,
    SEPTEMBER_REPORT,
    SEMINUAL_REPORT
)
from utils.api_utils import return_sync_get_soup
from utils.exception_utils import NotableError
from utils.class_utils import DataHandlerClass, BaseTest


@pytest.fixture(scope="module")
def browser():
    # ...
    # 브라우저를 받아오는 부분
    # driver는 webDriver이다.
    ddh = DartDataHandler
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

class FinanceTest(BaseTest, DataHandlerClass):

    def setUp(self):
        super().setUp()
        self.ddh = DartDataHandler('testdb')
    
    def tearDown(self):
        super().tearDown()
        


    def check_finance_table_dictionary(self, financial_table_data):
        data_key_list = ['balance_sheet', 'cashflow', 'income_statement']  
        for key in data_key_list:
            self.assertTrue(key in financial_table_data.keys())

    def test_return_report_link_list(self):
        stock_code = 5930
        company_name = "삼성전자"
        report_list = self.ddh.return_company_report_link_list(
            stock_code, 
            company_name,
            QUARTER_REPORT
        )
        self.assertTrue(len(report_list) >= 30)
        for report in report_list:
            self.assertEqual(type(report['link']), str)
            self.assertEqual(
                type(
                    dt.strptime(
                        report['reg_date'],
                        '%Y-%m-%d'
                    )
                ), 
                dt
            )
            if '03' in report['title']:
                self.assertEqual(report['period_type'], MARCH_REPORT)
            elif '09' in report['title']:
                self.assertEqual(report['period_type'], SEPTEMBER_REPORT)
            self.assertEqual(type(report['market_type']), str)
            self.assertEqual(type(report['title']), str)
            self.assertEqual(type(report['corp_name']), str)

    def test_return_seminual_report_link_list(self):
        stock_code = 5930
        company_name = "삼성전자"
        report_list = self.ddh.return_company_report_link_list(
            stock_code, 
            company_name,
            SEMINUAL_REPORT
        )
        self.assertTrue(len(report_list) >= 15)
        for report in report_list:
            self.assertEqual(type(report['link']), str)
            self.assertEqual(
                type(
                    dt.strptime(
                        report['reg_date'],
                        '%Y-%m-%d'
                    )
                ), 
                dt
            )
            self.assertEqual(report['period_type'], SEMINUAL_REPORT)
            self.assertEqual(type(report['market_type']), str)
            self.assertEqual(type(report['title']), str)
            self.assertEqual(type(report['corp_name']), str)

    def test_return_report_link_list_hyundai(self):
        stock_code = 5380
        company_name = "현대자동차"
        report_list = self.ddh.return_company_report_link_list(
            stock_code,
            company_name,
            YEARLY_REPORT
        )
        self.assertTrue(len(report_list)>=11)
        self.assertEqual(type(report_list[0]['link']),str)
        self.assertEqual(type(report_list[0]['reg_date']),dt)
        self.assertEqual(type(report_list[0]['market_type']),str)
        self.assertEqual(type(report_list[0]['title']),str)
        self.assertEqual(type(report_list[0]['corp_name']),str)

    def test_excel_data_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190401003281'
        result = self._run(self.ddh.return_reportlink_data(link=link))
        result_list, failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),2)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                self.assertEqual(result['current_assets'], 31246722206)
                self.assertEqual(
                    result['cashflow_from_operation'], 
                    18368166497
                )    
                self.assertEqual(result['total_assets'], 91648911057)
                self.assertEqual(result['longterm_debt'], 2095096477)
                self.assertEqual(result['current_debt'], 23870127281)
                self.assertEqual(result['book_value'], 65683687299)
                self.assertEqual(result['sales'], 75944381164)
                self.assertEqual(result['operational_income'], 8476299586)
                self.assertEqual(result['net_income'], 6524582683)
            else:
                self.assertEqual(result['current_assets'], 45900138047)
                self.assertEqual(
                    result['cashflow_from_operation'], 
                    26996888606
                )
                self.assertEqual(result['total_assets'], 166309402053)
                self.assertEqual(result['longterm_debt'], 11068559688)
                self.assertEqual(result['current_debt'], 65993597856)
                self.assertEqual(result['book_value'], 89247244509)
                self.assertEqual(result['sales'], 152690561553)
                self.assertEqual(result['operational_income'], 15774064694)
                self.assertEqual(result['net_income'], 10166296071)
        
    def test_return_finance_link_report_list(self):
        code = 311060
        date = '2018-12-28'
        reg_date = dt.strptime(date,"%Y-%m-%d")
        corp_name = '엘에이티'
        result = self.ddh.return_company_report_list(code,corp_name,reg_date)
        self.assertTrue(len(result)>=1)
        self.check_returned_report_link_data(result)

    def test_parse_eq_offer_list(self):
        stock_code = 5930
        company_name="삼성전자"
        eq_offer_list = self.ddh.return_company_eq_offer_lists(stock_code,company_name)
        self.assertEqual(len(eq_offer_list),1)
        self.assertTrue('reg_date' in eq_offer_list[0])
        # self.assertTrue('')
        


    def test_parsing_error_case(self):
        url = 'http://dart.fss.or.kr/report/viewer.do?rcpNo=20140515001710&dcmNo=4217491&eleId=27&offset=799724&length=219631&dtd=dart3.xsd'

    def test_eq_offer_api(self):
        stock_code = 5930
        company_name="삼성전자"
        eq_offer_list = self.ddh.return_company_eq_offer_lists(stock_code,company_name)
        self.assertEqual(len(eq_offer_list),1)
        self.assertTrue('reg_date' in eq_offer_list[0])



    def test_eq_offer_api(self):
        code = 5930
        company_name="삼성전자"
        post_data = {'company':company_name, 'code':code}
        response = self.client.post('/return_eq_api',json=post_data)
        eq_offer_list = json.loads(response.data)
        self.assertEqual(len(eq_offer_list),1)
        self.assertTrue('reg_date' in eq_offer_list[0])


    def test_statestment_with_noteincolumns(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190401004844'
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),1)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                self.assertEqual(result['current_assets'],12898820863)
                self.assertEqual(result['cashflow_from_operation'], -3134053492)    
                self.assertEqual(result['total_assets'], 24371975092)
                self.assertEqual(result['longterm_debt'],5109253256)
                self.assertEqual(result['current_debt'],3806369213)
                self.assertEqual(result['book_value'], 15456352623)
                self.assertEqual(result['sales'], 5448790067)
                self.assertEqual(result['operational_income'], -4603321664)
                self.assertEqual(result['net_income'],-40915428995)


    # def test_cashflow_third_error_case(self):
    #     link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20150331004620'
    #     self.check_quarter_data(link)


    # def test_error_fourth_case(self):
    #     link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190417000232'
    #     self.check_quarter_data(link)


    # def test_error_fith_case(self):
    #     link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20181106000173'
    #     self.check_quarter_data(link)
        
    

    # def test_error_sixth_case(self):
    #     link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190515001677'
    #     self.check_quarter_data(link)
        
    # def test_error_seventh_case(self):
    #     link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20170331004610'
    #     self.check_quarter_data(link)

    def test_where_multiple_columns_exists(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20030331001340'
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),1)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                self.assertEqual(result['current_assets'], 2269897851)
                self.assertEqual(result['cashflow_from_operation'], -1315509824)    
                self.assertEqual(result['total_assets'], 5269596317)
                self.assertEqual(result['longterm_debt'],2482274729)
                self.assertEqual(result['current_debt'], 899052938)
                self.assertEqual(result['sales'], 1174265504)
                self.assertEqual(result['operational_income'], -1128036308)
                self.assertEqual(result['net_income'],-1248898992)

        # self.check_two_period_data(link)
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20180402000209'
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),1)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                self.assertEqual(result['current_assets'], 26670483762)
                self.assertEqual(result['last_year_current_assets'], 19854422081)    
                self.assertEqual(result['last_year_current_debt'], 29744559545)    
                self.assertEqual(result['total_assets'], 43378342172)
                self.assertEqual(result['longterm_debt'],2428462140)
                self.assertEqual(result['current_debt'], 27600609347)
                self.assertEqual(result['sales'], 70377553599)
                self.assertEqual(result['operational_income'], 6386674207)
                self.assertEqual(result['net_income'],7307898456)
                self.assertEqual(result['cashflow_from_operation'], 10952979075)    
        

        # self.check_onecolumn_data(link
    def test_where_multiple_title_column(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20001115000043'
        result_list, failed_result = self._run(
            self.ddh.return_reportlink_data(link=link)
        )
        self.assertEqual(len(result_list), 1)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                self.assertEqual(result['current_assets'], 3149876688)
                self.assertEqual(result['total_assets'], 3894849693)
                self.assertEqual(result['longterm_debt'], 1000000000)
                self.assertEqual(result['current_debt'], 502720137)
                self.assertEqual(result['sales'], 6129323104)
                self.assertEqual(result['net_income'], -524010221)

    def test_driver_parsing_table(self):
        table_url = 'http://dart.fss.or.kr/report/viewer.do?rcpNo=20030407000694&dcmNo=582896&eleId=13134&offset=1542891&length=37875&dtd=dart2.dtd'
        soup = return_sync_get_soup(table_url)
        financial_table_data = (
            dartdataparsehandler
            .return_financial_report_table(
                table_url,
                soup
            )
        )
        result = dartdriverparsinghandler.return_driver_report_data(
            table_url, financial_table_data
        )
        expected_current_assets = 12079994
        cashflow_from_operation = 11193197
        total_assets = 34439600
        longterm_debt = 1710645
        current_debt = 8418665
        sales = 40511563
        net_income = 7051761
        operational_income = 7244672
        self.assertEqual(
            result['current_assets'], 
            expected_current_assets * 1000000
        )
        self.assertEqual(
            result['cashflow_from_operation'], 
            cashflow_from_operation * 1000000
        )    
        self.assertEqual(result['total_assets'],total_assets * 1000000)
        self.assertEqual(result['longterm_debt'],longterm_debt * 1000000)
        self.assertEqual(result['current_debt'],current_debt * 1000000)
        self.assertEqual(result['sales'], sales * 1000000)
        self.assertEqual(result['operational_income'], operational_income * 1000000)
        self.assertEqual(result['net_income'],net_income*1000000)

    def test_driver_parsing_list_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20030407000694'
        result_list, failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),2)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                expected_current_assets = 12079994
                cashflow_from_operation = 11193197
                total_assets = 34439600
                longterm_debt = 1710645
                current_debt = 8418665
                total_book_value= 24310290
                sales = 40511563
                net_income = 7051761
                operational_income = 7244672
                self.assertEqual(result['current_assets'],expected_current_assets*1000000)
                self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation*1000000)    
                self.assertEqual(result['total_assets'],total_assets*1000000)
                self.assertEqual(result['longterm_debt'],longterm_debt*1000000)
                self.assertEqual(result['current_debt'],current_debt*1000000)
                self.assertEqual(result['book_value'],total_book_value*1000000)
                self.assertEqual(result['sales'],sales*1000000)
                self.assertEqual(result['operational_income'],operational_income*1000000)
                self.assertEqual(result['net_income'],net_income*1000000)

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
                self.assertEqual(result['current_assets'],expected_current_assets*1000000)
                self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation*1000000)    
                self.assertEqual(result['total_assets'],total_assets*1000000)
                self.assertEqual(result['longterm_debt'],longterm_debt*1000000)
                self.assertEqual(result['current_debt'],current_debt*1000000)
                self.assertEqual(result['book_value'],total_book_value*1000000)
                self.assertEqual(result['sales'],sales*1000000)
                self.assertEqual(result['operational_income'],operational_income*1000000)
                self.assertEqual(result['net_income'],net_income*1000000)

    def test_samsung_error_data_parsign(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20000330000796'
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),2)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                self.assertEqual(result['current_assets'], 6197231279000)
                self.assertEqual(result['cashflow_from_operation'], 7077732141000)    
                self.assertEqual(result['total_assets'], 24709802882000)
                self.assertEqual(result['longterm_debt'],4597362974000)
                self.assertEqual(result['current_debt'],6780871440000)
                self.assertEqual(result['sales'],26117785751000)
                self.assertEqual(result['operational_income'],4481500117000)
                self.assertEqual(result['net_income'],3170402574000)

    def test_table_data_parsing(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190515000764'
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),2)
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
                self.assertEqual(result['current_assets'],expected_current_assets)
                self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation)    
                self.assertEqual(result['total_assets'],total_assets)
                self.assertEqual(result['longterm_debt'],longterm_debt)
                self.assertEqual(result['current_debt'],current_debt)
                self.assertEqual(result['sales'],sales)
                self.assertEqual(result['operational_income'],operational_income)
                self.assertEqual(result['net_income'],net_income)

            else:
                expected_current_assets = 48847498882
                cashflow_from_operation = 12460770632
                total_assets = 173536441461
                longterm_debt = 12384792007
                current_debt = 70635438719
                sales = 44091600036
                net_income = 4843725571
                operational_income = 6709454095
                self.assertEqual(result['current_assets'],expected_current_assets)
                self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation)    
                self.assertEqual(result['total_assets'],total_assets)
                self.assertEqual(result['longterm_debt'],longterm_debt)
                self.assertEqual(result['current_debt'],current_debt)
                self.assertEqual(result['sales'],sales)
                self.assertEqual(result['operational_income'],operational_income)
                self.assertEqual(result['net_income'],net_income)


    def test_korean_air_report(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20160516003079'
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),2)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                self.assertEqual(result['current_assets'], 2873562672777)
                self.assertEqual(result['cashflow_from_operation'], 2666464423603)
                self.assertEqual(result['total_assets'], 23048939945281)
                self.assertEqual(result['longterm_debt'], 12324531908724)
                self.assertEqual(result['current_debt'], 8428114074534)
                self.assertEqual(result['sales'], 11308423372686)
                self.assertEqual(result['operational_income'], 859217822453)
                self.assertEqual(result['net_income'], -407682308362)

            else:
                self.assertEqual(result['current_assets'], 3289127053478)
                self.assertEqual(result['cashflow_from_operation'],2728023077614)    
                self.assertEqual(result['total_assets'], 24180351112715)
                self.assertEqual(result['longterm_debt'], 13230934646401)
                self.assertEqual(result['current_debt'], 8450381325032)
                self.assertEqual(result['sales'],11544831301113)
                self.assertEqual(result['operational_income'], 883088280640)
                self.assertEqual(result['net_income'], -562967287220)

    def test_total_stock_count(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20160516003079'
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),2)
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
                self.assertEqual(result['current_assets'],expected_current_assets)
                self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation)    
                self.assertEqual(result['total_assets'],total_assets)
                self.assertEqual(result['longterm_debt'],longterm_debt)
                self.assertEqual(result['current_debt'],current_debt)
                self.assertEqual(result['sales'],sales)
                self.assertEqual(result['operational_income'],operational_income)
                self.assertEqual(result['net_income'],net_income)
                self.assertEqual(result['preferred_stock_count'],1110794)

            else:
                expected_current_assets = 174697424
                cashflow_from_operation = 67031863
                total_assets = 339357244
                longterm_debt = 22522557
                current_debt = 69081510
                sales = 243771415
                net_income = -562967287220
                operational_income = 883088280640
                self.assertEqual(result['current_assets'], 3289127053478)
                self.assertEqual(result['cashflow_from_operation'], 2728023077614)    
                self.assertEqual(result['total_assets'], 24180351112715)
                self.assertEqual(result['longterm_debt'],13230934646401)
                self.assertEqual(result['current_debt'],8450381325032)
                self.assertEqual(result['sales'],11544831301113)
                self.assertEqual(result['operational_income'],883088280640)
                self.assertEqual(result['net_income'], -562967287220)
                self.assertEqual(result['common_stock_count'],72839744)
                self.assertEqual(result['preferred_stock_count'],1110794)

    def test_excel_data_parsing(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190401004781'
        soup = return_sync_get_soup(link)
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),2)
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
                self.assertEqual(result['current_assets'],expected_current_assets*1000000)
                self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation*1000000)    
                self.assertEqual(result['total_assets'],total_assets*1000000)
                self.assertEqual(result['longterm_debt'],longterm_debt*1000000)
                self.assertEqual(result['current_debt'],current_debt*1000000)
                self.assertEqual(result['sales'],sales*1000000)
                self.assertEqual(result['operational_income'],operational_income*1000000)
                self.assertEqual(result['net_income'],net_income*1000000)

            else:
                expected_current_assets = 174697424
                cashflow_from_operation = 67031863
                total_assets = 339357244
                longterm_debt = 22522557
                current_debt = 69081510
                sales = 243771415
                net_income = 44344857
                operational_income = 58886669
                self.assertEqual(result['current_assets'],expected_current_assets*1000000)
                self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation*1000000)    
                self.assertEqual(result['total_assets'],total_assets*1000000)
                self.assertEqual(result['longterm_debt'],longterm_debt*1000000)
                self.assertEqual(result['current_debt'],current_debt*1000000)
                self.assertEqual(result['sales'],sales*1000000)
                self.assertEqual(result['operational_income'],operational_income*1000000)
                self.assertEqual(result['net_income'],net_income*1000000)


    def test_error_case_laontech(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20200330002340'
        soup = return_sync_get_soup(link)
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),1)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                expected_current_assets = 7735651632
                cashflow_from_operation = -285756285
                total_assets = 18754930175
                longterm_debt = 2888179
                current_debt = 43145053
                self.assertEqual(result['current_assets'],expected_current_assets)
                self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation)    
                self.assertEqual(result['total_assets'],total_assets)
                self.assertEqual(result['longterm_debt'],9673179122)
                self.assertEqual(result['current_debt'],5784704873)
                self.assertEqual(result['sales'],12632423500)
                self.assertEqual(result['operational_income'],-1211469053)
                self.assertEqual(result['net_income'], -1431915408)
                self.assertEqual(result['book_value'],3297046180)


    def test_laontech_second_error_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190401002805'
        soup = return_sync_get_soup(link)
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),1)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                expected_current_assets = 12074416412
                self.assertEqual(result['current_assets'],expected_current_assets)
                
    def test_laontech_third_error_case(self): 
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20151215000033'
        soup = return_sync_get_soup(link)
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),1)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                expected_current_assets = 3767375000
                self.assertEqual(result['current_assets'],expected_current_assets)
                self.assertEqual(result['cashflow_from_operation'],-361962000)
                self.assertEqual(result['net_income'],539589000)


    def test_hanzin_first_error_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20150331003586'
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),2)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                expected_current_assets = 7735651632
                cashflow_from_operation = -285756285
                total_assets = 18754930175
                longterm_debt = 2888179
                current_debt = 43145053
                self.assertEqual(result['current_assets'],24374000000)
                self.assertEqual(result['cashflow_from_operation'],9367000000)    
                self.assertEqual(result['total_assets'],1124296000000)
                self.assertEqual(result['longterm_debt'],34514000000)
                self.assertEqual(result['current_debt'],30904000000)
                self.assertEqual(result['sales'],15091000000)
                self.assertEqual(result['operational_income'],11061000000)
                self.assertEqual(result['net_income'], 9092000000)
                self.assertEqual(result['book_value'],1058878000000)
            else:
                self.assertEqual(result['current_assets'], 456871000000)
                self.assertEqual(result['cashflow_from_operation'], 50230000000)    
                self.assertEqual(result['total_assets'], 2722203000000)
                self.assertEqual(result['longterm_debt'],1044949000000)
                self.assertEqual(result['current_debt'],569946000000)
                self.assertEqual(result['sales'],1520058000000)
                self.assertEqual(result['operational_income'],-79709000000)
                self.assertEqual(result['net_income'], -135021000000)
                self.assertEqual(result['book_value'], 1107308000000)



    def test_hanzin_tech_error_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20040510001670'
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.assertEqual(len(result_list),1)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                expected_current_assets = 7735651632
                cashflow_from_operation = -285756285
                total_assets = 18754930175
                longterm_debt = 2888179
                current_debt = 43145053
                self.assertEqual(result['current_assets'],2018848156000)
                self.assertEqual(result['cashflow_from_operation'],847485458000)    
                self.assertEqual(result['total_assets'],3547476247000)
                self.assertEqual(result['longterm_debt'],1448125545000)
                self.assertEqual(result['current_debt'],1048617907000)
                self.assertEqual(result['sales'],1543948233000)
                self.assertEqual(result['operational_income'],85408690000)
                self.assertEqual(result['net_income'], 24758249000)
                self.assertEqual(result['book_value'],1050732795000)


    def test_shinhan_report_parsing(self):
        url = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20121026000269'
        result_list, failed_result = self._run(self.ddh.return_reportlink_data(link=url))
        self.assertEqual(len(result_list),2)
        for result in result_list:
            if result.get("report_type") == 'NORMAL_FINANCIAL_STATEMENTS':
                self.assertEqual(result['cashflow_from_operation'], 1343507000000)    
                self.assertEqual(result['total_assets'], 30844250000000)
                self.assertEqual(result['operational_income'],1679986000000)
                self.assertEqual(result['net_income'], 1672908000000)
                self.assertEqual(result['book_value'],19430807000000)
            else:
                self.assertEqual(result['cashflow_from_operation'], 1343507000000)    
                self.assertEqual(result['total_assets'], 288041796000000)
                self.assertEqual(result['operational_income'], 4134772000000)
                self.assertEqual(result['net_income'], 3272633000000)
                self.assertEqual(result['book_value'], 26858805000000)

    def test_failed_flower_firms(self):
        url = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20100503001219'
        result_list, failed_result = self._run(self.ddh.return_reportlink_data(link=url))
        self.assertEqual(len(result_list), 1)
        result = result_list[0]
        self.assertEqual(result['current_assets'], 350604075195)
        self.assertEqual(result['cashflow_from_operation'],127791674884)    
        self.assertEqual(result['total_assets'], 751085360770)
        self.assertEqual(result['longterm_debt'], 45977870766)
        self.assertEqual(result['current_debt'], 205753622377)
        self.assertEqual(result['sales'], 718165366951)
        self.assertEqual(result['operational_income'], 64264962312)
        self.assertEqual(result['net_income'], 54845066144)
        self.assertEqual(result['book_value'], 499353867627)
