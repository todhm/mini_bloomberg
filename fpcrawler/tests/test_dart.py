from datetime import datetime as dt
import os 
import json
from random import randint
import unittest
from unittest.mock import patch
from datahandler.dartdatahandler import DartDataHandler
from fp_types import YEARLY_REPORT
from utils.api_utils import return_sync_get_soup
from utils.exception_utils import NotableError
from utils.class_utils import DataHandlerClass, BaseTest





def save_report_list_data(company_report_col, **params):
    defaults = {
    "code" : '3670', 
    "link" : "http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20060512002342", 
    "reg_date" : '2018-11-11',
    "corp_name" : "포스코케미칼", 
    "market_type" : "유가증권시장", 
    "title" : "[기재정정]사업보고서 (2005.12)", 
    "period_type" : "사업보고서",
     "reporter" : "포스코케미칼"
    }
    defaults.update(params)
    company_report_col.insert_one(defaults)
    return defaults


class DartTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ddh = DartDataHandler('testdb')

        


    def test_dart_link_parse(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20121114000100'
        soup = return_sync_get_soup(link)
        data = self.ddh.parse_financial_section_link(soup)
        

    def tearDown(self):
        super().tearDown()
        



    
class FinanceTest(BaseTest, DataHandlerClass):
    def setUp(self):
        super().setUp()
        self.ddh = DartDataHandler('testdb')

        
    def tearDown(self):
        super().tearDown()
        

    def check_return_result(self,result_list,company_name="",code=""):
        data_field_list = [
            'cashflow_from_operation',
            'operational_income',
            'gross_profit',
            'sales',
            'extra_ordinary_profit',
            'extra_ordinary_loss',
            'net_income',
            'total_assets',
            'longterm_debt',
            'current_assets',
            'current_debt'
        ]
        
        self.assertTrue(len(result_list)>0)
        for result in result_list:
            for key in data_field_list:
                self.assertTrue(type(result[key]),float)
            if company_name:
                self.assertEqual(result['corp_name'],company_name)
                self.assertEqual(result['code'],code)


    def check_returned_report_link_data(self,data_list):
        for data in data_list:
            self.assertTrue('http' in data['link'])
            self.assertTrue(data['code'].isdigit())
            self.assertTrue(data['market_type'])
            self.assertTrue(data['market_type'])
            self.assertEqual(type(data['reg_date']),dt)


    def check_finance_table_dictionary(self,financial_table_data):
        data_key_list = ['balance_sheet','cashflow','income_statement']  
        for key in data_key_list:
            self.assertTrue(key in financial_table_data.keys())

    

    def check_quarter_data(self,link):
        pass
        # code = self.save_temp_object(link)
        # self.ddh.save_company_all_financial_statements_data(code)
        # data_list = list(self.db.company_data_list.find({"company_code":code}))
        # data = data_list[0]
        # before_year_asset = data['balancesheet_period_data_list'][1]['asset_sum']
        # roa_son = data['net_income'] - data['special_income'] + data['special_loss']
        # self.assertTrue(type(roa_son/before_year_asset),float)



    def check_onecolumn_data(self,link):
        pass
        # code = self.save_temp_object(link)
        # self.ddh.save_company_all_financial_statements_data(code)
        # data_list = list(self.db.company_data_list.find({"company_code":code}))
        # data = data_list[0]
        # current_year_asset = data['asset_sum']

        # roa_son = data['net_income'] - data['special_income'] + data['special_loss']

        # long_term_debt = data['long_term_debt']
        # current_asset = data['asset_current']
        # if current_asset == 0 :
        #     current_asset += data['asset_current_sub1']
        #     current_asset += data['asset_current_sub2']
        #     current_asset += data['asset_current_sub3']

        # current_year_liabilities = data['liability_current']
        # if current_year_liabilities == 0 :
        #     current_year_liabilities += data['liability_current_sub1']
        #     current_year_liabilities += data['liability_current_sub2']
        #     current_year_liabilities += data['liability_current_sub3']
        # current_year_gross_margin = data['gross_margin']
        # current_year_gross_margin -= data['gross_loss']

        # if current_year_gross_margin == 0:
        #     current_year_gross_margin = data['op_income'] + data['other_sales'] - data['other_costs']

        # current_year_total_sales = data['sales'] 
        # if current_year_total_sales ==0 :
        #     current_year_total_sales = data['sales2'] + data['other_sales']
        # self.assertTrue(current_asset>0)
        # self.assertTrue(current_year_gross_margin!=0)

    def check_two_period_data(self,link):
        pass
        # code = self.save_temp_object(link)
        # self.ddh.save_company_all_financial_statements_data(code)
        # data_list = list(self.db.company_data_list.find({'company_code':code}))
        # data = data_list[0]
        # current_year_asset = data['asset_sum']
        # before_year_asset = data['balancesheet_period_data_list'][1]['asset_sum']
        # before_year_income_data = data['income_period_data_list'][1]

        # current_net_income = data['net_income'] if data.get('net_income') and data.get('net_income') !=0 else data.get('net_loss') * -1
        # roa_son = current_net_income - data['special_income'] + data['special_loss']
        # last_year_special_income = before_year_income_data['special_income'] if before_year_income_data.get('special_income') else 0 
        # last_year_special_loss = before_year_income_data['special_loss'] if before_year_income_data.get('special_loss') else 0 
        # before_year_net_income = before_year_income_data['net_income'] if before_year_income_data.get('net_income',0) and before_year_income_data.get('net_income',0) !=0 else before_year_income_data.get('net_loss',0) * -1
        # roa_last_year_son = before_year_net_income - last_year_special_income - last_year_special_loss

        # average_total_year_asset = (current_year_asset+before_year_asset)/2
        # long_term_debt = data['long_term_debt']
        # leverage = long_term_debt / average_total_year_asset
        # last_year_long_term_debt = data['balancesheet_period_data_list'][1]['asset_sum']
        # average_total_lastyear_asset = (before_year_asset+last_year_long_term_debt)/2
        # last_year_leverage = last_year_long_term_debt/average_total_lastyear_asset
        # leverage_delta = leverage - last_year_leverage

        # current_asset = data['asset_current']
        # if current_asset == 0 :
        #     current_asset += data['asset_current_sub1']
        #     current_asset += data['asset_current_sub2']
        #     current_asset += data['asset_current_sub3']

        # current_year_liabilities = data['liability_current']
        # if current_year_liabilities == 0 :
        #     current_year_liabilities += data['liability_current_sub1']
        #     current_year_liabilities += data['liability_current_sub2']
        #     current_year_liabilities += data['liability_current_sub3']

        # current_ratio = current_asset/current_year_liabilities
        # last_year_current_asset = data['balancesheet_period_data_list'][1].get('asset_current')
        # lb = data['balancesheet_period_data_list'][1]
        # last_year_current_asset = last_year_current_asset if last_year_current_asset else 0
        # if last_year_current_asset ==0:
        #     last_year_current_asset += lb['asset_current_sub1'] if lb.get('asset_current_sub1') else 0
        #     last_year_current_asset += lb['asset_current_sub2'] if lb.get('asset_current_sub2') else 0
        #     last_year_current_asset += lb['asset_current_sub3'] if lb.get('asset_current_sub3') else 0
        # last_year_current_liabilities = lb.get('liability_current')
        # last_year_current_liabilities = last_year_current_liabilities if last_year_current_liabilities else 0

        # if last_year_current_liabilities ==0 :
        #     last_year_current_liabilities += lb['liability_current_sub1'] if lb.get('liability_current_sub1') else 0
        #     last_year_current_liabilities += lb['liability_current_sub2'] if lb.get('liability_current_sub2') else 0
        #     last_year_current_liabilities += lb['liability_current_sub3'] if lb.get('liability_current_sub3') else 0


        # last_year_current_ratio = last_year_current_asset/last_year_current_liabilities
        # liquid_delta = current_ratio - last_year_current_ratio

        # common_stock_equity = data['common_stock_equity']
        # lastyear_common_stock_equity = lb['common_stock_equity'] if lb.get('common_stock_equity') else 0 
        # eq_offer = not (common_stock_equity - lastyear_common_stock_equity) >0

        # cfo_son = data['op_cashflow']
        # if cfo_son ==0:
        #     cfo_son = data['net_income'] + data['op_asset_debt_change'] + data['op_cash_add_cost']+data['op_none_cash_income']

        # cfo = cfo_son/before_year_asset
        # accurual_son = roa_son - cfo_son
        # accurual  = accurual_son/before_year_asset
        # roa = roa_son/before_year_asset

        # current_year_gross_margin = data['gross_margin']
        # current_year_gross_margin -= data['gross_loss']

        # if current_year_gross_margin == 0:
        #     current_year_gross_margin = data['op_income'] + data['other_sales'] - data['other_costs']
        # current_year_total_sales = data['sales'] 
        # if current_year_total_sales ==0 :
        #     current_year_total_sales = data['sales2'] + data['other_sales'] +data['sales_finance']

        # last_year_income_data = data['income_period_data_list'][1]
        # current_year_gmo = current_year_gross_margin/current_year_total_sales
        # last_year_gross_margin = last_year_income_data['gross_margin'] if last_year_income_data.get('gross_margin') else 0 
        # last_year_gross_loss = last_year_income_data['gross_loss'] if last_year_income_data.get('gross_loss') else 0 
        # last_year_gross_margin -= last_year_gross_loss

        # if last_year_gross_margin ==0 :
        #     last_year_gross_margin = last_year_income_data['op_income'] + last_year_income_data.get('other_sales',0) - last_year_income_data.get('other_costs',0)


        # last_year_total_sales = last_year_income_data['sales'] if last_year_income_data.get('sales') else 0 
        # if last_year_total_sales ==0 :
        #     last_year_saels_finance = last_year_income_data.get('sales_finance',0)
        #     last_year_total_sales = last_year_income_data['sales2'] + last_year_income_data.get('other_sales',0)+last_year_saels_finance

        # last_year_gmo = last_year_gross_margin / last_year_total_sales
        # gmo_delta = current_year_gmo - last_year_gmo

        # atr_this_year = current_year_total_sales/before_year_asset

        # self.assertTrue(cfo_son>0)
        # self.assertTrue(type(roa),float)
        # self.assertTrue(type(cfo),float)
        # self.assertTrue(type(accurual),float)
        # self.assertTrue(type(leverage_delta),float)
        # self.assertTrue(type(liquid_delta),float)
        # self.assertTrue(type(gmo_delta),float)
        # self.assertTrue(current_year_gross_margin!=0)
        # self.assertTrue(last_year_gross_margin!=0)




    def check_annual_data(self,link):
        pass
        # code = self.save_temp_object(link)
        # self.ddh.save_company_all_financial_statements_data(code)
        # data = data_list[0]
        # current_year_asset = data['asset_sum']
        # before_year_asset = data['balancesheet_period_data_list'][1]['asset_sum']
        # before_year_before_asset = data['balancesheet_period_data_list'][2]['asset_sum']

        # roa_son = data['net_income'] - data['special_income'] + data['special_loss']
        # roa_last_year_son = data['net_income'] - data['special_income'] + data['special_loss']
        # roa_last_year = roa_last_year_son/before_year_before_asset

        # average_total_year_asset = (current_year_asset+before_year_asset)/2
        # long_term_debt = data['long_term_debt']
        # leverage = long_term_debt / average_total_year_asset
        # last_year_long_term_debt = data['balancesheet_period_data_list'][1]['asset_sum']
        # average_total_lastyear_asset = (before_year_asset+last_year_long_term_debt)/2
        # last_year_leverage = last_year_long_term_debt/average_total_lastyear_asset
        # leverage_delta = leverage - last_year_leverage

        # current_asset = data['asset_current']
        # if current_asset == 0 :
        #     current_asset += data['asset_current_sub1']
        #     current_asset += data['asset_current_sub2']
        #     current_asset += data['asset_current_sub3']

        # current_year_liabilities = data['liability_current']
        # if current_year_liabilities == 0 :
        #     current_year_liabilities += data['liability_current_sub1']
        #     current_year_liabilities += data['liability_current_sub2']
        #     current_year_liabilities += data['liability_current_sub3']

        # current_ratio = current_asset/current_year_liabilities
        # last_year_current_asset = data['balancesheet_period_data_list'][1].get('asset_current')
        # lb = data['balancesheet_period_data_list'][1]
        # last_year_current_asset = last_year_current_asset if last_year_current_asset else 0
        # if last_year_current_asset ==0:
        #     last_year_current_asset += lb['asset_current_sub1'] if lb.get('asset_current_sub1') else 0
        #     last_year_current_asset += lb['asset_current_sub2'] if lb.get('asset_current_sub2') else 0
        #     last_year_current_asset += lb['asset_current_sub3'] if lb.get('asset_current_sub3') else 0
        # last_year_current_liabilities = lb.get('liability_current')
        # last_year_current_liabilities = last_year_current_liabilities if last_year_current_liabilities else 0

        # if last_year_current_liabilities ==0 :
        #     last_year_current_liabilities += lb['liability_current_sub1'] if lb.get('liability_current_sub1') else 0
        #     last_year_current_liabilities += lb['liability_current_sub2'] if lb.get('liability_current_sub2') else 0
        #     last_year_current_liabilities += lb['liability_current_sub3'] if lb.get('liability_current_sub3') else 0


        # last_year_current_ratio = last_year_current_asset/last_year_current_liabilities
        # liquid_delta = current_ratio - last_year_current_ratio

        # common_stock_equity = data['common_stock_equity']
        # lastyear_common_stock_equity = lb['common_stock_equity'] if lb.get('common_stock_equity') else 0 
        # eq_offer = not (common_stock_equity - lastyear_common_stock_equity) >0

        # cfo_son = data['op_cashflow']
        # cfo = cfo_son/before_year_asset
        # accurual_son = roa_son - cfo_son
        # accurual  = accurual_son/before_year_asset
        # roa = roa_son/before_year_asset
        # roa_diff = roa - roa_last_year

        # current_year_gross_margin = data['gross_margin']
        # current_year_gross_margin -= data['gross_loss']

        # if current_year_gross_margin == 0:
        #     current_year_gross_margin = data['op_income'] + data['other_sales'] - data['other_costs']

        # current_year_total_sales = data['sales'] 
        # if current_year_total_sales ==0 :
        #     current_year_total_sales = data['sales2'] + data['other_sales']
        # last_year_income_data = data['income_period_data_list'][1]
        # current_year_gmo = current_year_gross_margin/current_year_total_sales
        # last_year_gross_margin = last_year_income_data['gross_margin'] if last_year_income_data.get('gross_margin') else 0 
        # last_year_gross_loss = last_year_income_data['gross_loss'] if last_year_income_data.get('gross_loss') else 0 
        # last_year_gross_margin -= last_year_gross_loss

        # if last_year_gross_margin ==0 :
        #     last_year_gross_margin = last_year_income_data['op_income'] + last_year_income_data['other_sales'] - last_year_income_data['other_costs']

        # last_year_total_sales = last_year_income_data['sales'] if last_year_income_data.get('sales') else 0 
        # if last_year_total_sales ==0 :
        #     last_year_total_sales = last_year_income_data['sales2'] + last_year_income_data['other_sales']

        # last_year_gmo = last_year_gross_margin / last_year_total_sales
        # gmo_delta = current_year_gmo - last_year_gmo

        # atr_this_year = current_year_total_sales/before_year_asset
        # atr_last_year = last_year_total_sales/before_year_before_asset
        # atr_delta = atr_this_year - atr_last_year
        # self.assertTrue(cfo_son>0)
        # self.assertTrue(type(roa),float)
        # self.assertTrue(type(cfo),float)
        # self.assertTrue(type(accurual),float)
        # self.assertTrue(type(roa_diff),float)
        # self.assertTrue(type(leverage_delta),float)
        # self.assertTrue(type(liquid_delta),float)
        # self.assertTrue(type(gmo_delta),float)
        # self.assertTrue(type(atr_delta),float)
        # self.assertTrue(current_year_gross_margin!=0)
        # self.assertTrue(last_year_gross_margin!=0)

    def test_return_report_list_api(self):
        company_name = '엘에이티'
        code = '311060'

        post_data = {'company':company_name,'code':code}
        response = self.client.post('/company_report_data_list',json=post_data)
        result_list = json.loads(response.data)
        self.assertEqual(response.status_code,200)
        self.check_return_result(result_list,company_name=company_name,code=code)
        
    
    def test_return_report_link_list(self):
        stock_code = 5930
        company_name="삼성전자"
        report_list = self.ddh.return_company_report_link_list(stock_code,company_name)
        self.assertTrue(len(report_list)>=60)
        self.assertEqual(type(report_list[0]['link']),str)
        self.assertEqual(type(report_list[0]['reg_date']),dt)
        self.assertEqual(type(report_list[0]['market_type']),str)
        self.assertEqual(type(report_list[0]['title']),str)
        self.assertEqual(type(report_list[0]['corp_name']),str)

    def test_return_report_link_list_hyundai(self):
        stock_code = 5380
        company_name="현대자동차"
        report_list = self.ddh.return_company_report_link_list(stock_code,company_name,YEARLY_REPORT)
        self.assertTrue(len(report_list)>=11)
        self.assertEqual(type(report_list[0]['link']),str)
        self.assertEqual(type(report_list[0]['reg_date']),dt)
        self.assertEqual(type(report_list[0]['market_type']),str)
        self.assertEqual(type(report_list[0]['title']),str)
        self.assertEqual(type(report_list[0]['corp_name']),str)
    def test_return_company_report_data_list(self):
        stock_code = 5930
        company_name="삼성전자"
        data_list = self.ddh.return_company_report_data_list(stock_code,company_name)
        print(data_list)
        
    def test_error_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190401003281'
        result = self._run(self.ddh.return_reportlink_data(link=link))
        self.check_return_result(result)
    
    def test_second_error_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190515000764'
        result = self._run(self.ddh.return_reportlink_data(link=link))
        self.check_return_result(result)
    

    def test_third_error_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20100331001680'
        result,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
        self.check_return_result(result)
        self.assertTrue(result[0]['cashflow_from_operation']>0)


    def test_complicated_table_case(self):
        link = 'http://dart.fss.or.kr/report/viewer.do?rcpNo=20000330000068&dcmNo=37890&eleId=3593&offset=399014&length=227055&dtd=dart2.dtd'
        financial_table_data = self.ddh.return_financial_report_table(link)
        self.check_finance_table_dictionary(financial_table_data)
        for key in financial_table_data:
            self.assertEqual(financial_table_data[key]['unit'],1)


        
    def test_return_finance_link_report_list(self):
        code = 311060
        date = '2018-12-28'
        reg_date = dt.strptime(date,"%Y-%m-%d")
        corp_name = '엘에이티'
        result = self.ddh.return_company_report_list(code,corp_name,reg_date)
        self.assertTrue(len(result)>=1)
        self.check_returned_report_link_data(result)

    def test_balancesheet_error_case(self):
        link = 'http://dart.fss.or.kr/report/viewer.do?rcpNo=20050331002046&dcmNo=1049370&eleId=6415&offset=715476&length=306592&dtd=dart2.dtd'
        soup = return_sync_get_soup(link)
        link_data = self.ddh.parse_report_link(link,soup)

    def test_parse_finance_link_soup(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190418000453'
        soup = return_sync_get_soup(link)
        link_data = self.ddh.parse_financial_section_link(soup)
        self.assertTrue(len(link_data.get('link_fs'))>0)
        self.assertTrue(len(link_data.get('link_connected_fs'))>0)


    def test_return_finance_report_table(self):
        link = 'http://dart.fss.or.kr/report/viewer.do?rcpNo=20190418000453&dcmNo=6704575&eleId=17&offset=255858&length=89703&dtd=dart3.xsd'
        soup = return_sync_get_soup(link)
        financial_table_data = self.ddh.return_financial_report_table(link,soup)
        self.check_finance_table_dictionary(financial_table_data)
        for key in financial_table_data:
            self.assertEqual(financial_table_data[key]['unit'],1)


    def test_parse_cashflow_table(self):
        link = 'http://dart.fss.or.kr/report/viewer.do?rcpNo=20190418000453&dcmNo=6704575&eleId=17&offset=255858&length=89703&dtd=dart3.xsd'
        financial_table_data = self.ddh.return_financial_report_table(link)
        cashflow_table = financial_table_data['cashflow']['table']
        unit = financial_table_data['cashflow']['unit']
        result = self.ddh.parse_cashflow_table(cashflow_table,unit)
        
    def test_return_finance_none_exists_table(self):
        link = 'http://dart.fss.or.kr/report/viewer.do?rcpNo=20190418000453&dcmNo=6704575&eleId=15&offset=255535&length=156&dtd=dart3.xsd'
        self.assertRaises(NotableError, self.ddh.return_financial_report_table, link)

    def test_return_finance_report_table_with_none_table_header(self):
        link ='http://dart.fss.or.kr/report/viewer.do?rcpNo=20000515000236&dcmNo=59796&eleId=3334&offset=401521&length=54670&dtd=dart2.dtd'
        soup = return_sync_get_soup(link)
        financial_table_data = self.ddh.return_financial_report_table(link,soup)
        self.check_finance_table_dictionary(financial_table_data)


    def test_parse_big_unit_finance_table_parser(self):
        link = 'http://dart.fss.or.kr/report/viewer.do?rcpNo=20050331002046&dcmNo=1049370&eleId=6415&offset=715476&length=306592&dtd=dart2.dtd'
        soup = return_sync_get_soup(link)
        financial_table_data = self.ddh.return_financial_report_table(link,soup)
        self.check_finance_table_dictionary(financial_table_data)
        for key in financial_table_data:
            self.assertEqual(financial_table_data[key]['unit'],1000000)


    def test_parse_eq_offer_list(self):
        stock_code = 5930
        company_name="삼성전자"
        eq_offer_list = self.ddh.return_company_eq_offer_lists(stock_code,company_name)
        self.assertEqual(len(eq_offer_list),1)
        self.assertTrue('reg_date' in eq_offer_list[0])
        # self.assertTrue('')
        




    def test_eq_offer_api(self):
        stock_code = 5930
        company_name="삼성전자"
        eq_offer_list = self.ddh.return_company_eq_offer_lists(stock_code,company_name)
        self.assertEqual(len(eq_offer_list),1)
        self.assertTrue('reg_date' in eq_offer_list[0])



    def test_eq_offer_api(self):
        code = 5930
        company_name="삼성전자"
        post_data = {'company':company_name,'code':code}
        response = self.client.post('/return_eq_api',json=post_data)
        eq_offer_list = json.loads(response.data)
        self.assertEqual(len(eq_offer_list),1)
        self.assertTrue('reg_date' in eq_offer_list[0])


    def test_statestment_with_noteincolumns(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190401004844'
        self.check_quarter_data(link)

    def test_cashflow_third_error_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20150331004620'
        self.check_quarter_data(link)


    def test_error_fourth_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190417000232'
        self.check_quarter_data(link)


    def test_error_fith_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20181106000173'
        self.check_quarter_data(link)
        
    

    def test_error_sixth_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190515001677'
        self.check_quarter_data(link)
        
    def test_error_seventh_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20170331004610'
        self.check_quarter_data(link)

    def test_where_multiple_columns_exists(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20030331001340'
        self.check_two_period_data(link)
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20180402000209'
        self.check_onecolumn_data(link)


    def test_where_multiple_title_column(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20001115000043'
        self.check_two_period_data(link)




    def test_income_parse_error(self):
        link ='http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20061124000207'
        self.check_two_period_data(link)


    def test_only_one_datacolumn_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190603000157'
        self.check_onecolumn_data(link)


    def test_find_value_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20180514003685'
        self.check_two_period_data(link)
        link ='http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20171031000424'
        self.check_annual_data(link)

    def test_connected_error_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20171114000945'
        self.check_two_period_data(link)


    def test_connected_none_table(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20000515000236'
        self.check_annual_data(link)

    def test_driver_parsing_table(self):
        table_url = 'http://dart.fss.or.kr/report/viewer.do?rcpNo=20030407000694&dcmNo=582896&eleId=13134&offset=1542891&length=37875&dtd=dart2.dtd'
        soup = return_sync_get_soup(table_url)
        financial_table_data = self.ddh.return_financial_report_table(table_url,soup)
        result = self.ddh.return_driver_report_data(table_url, financial_table_data)
        expected_current_assets = 12079994
        cashflow_from_operation = 11193197
        total_assets = 34439600
        longterm_debt = 1710645
        current_debt = 8418665
        sales = 40511563
        net_income = 7051761
        operational_income = 7244672
        self.assertEqual(result['current_assets'],expected_current_assets*1000000)
        self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation*1000000)    
        self.assertEqual(result['total_assets'],total_assets*1000000)
        self.assertEqual(result['longterm_debt'],longterm_debt*1000000)
        self.assertEqual(result['current_debt'],current_debt*1000000)
        self.assertEqual(result['sales'],sales*1000000)
        self.assertEqual(result['operational_income'],operational_income*1000000)
        self.assertEqual(result['net_income'],net_income*1000000)

    def test_driver_parsing_list_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20030407000694'
        result_list,failed_result = self._run(self.ddh.return_reportlink_data(link=link))
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
            if result.get("report_type") != 'NORMAL_FINANCIAL_STATEMENTS':
                print(result)
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
                expected_current_assets = 2873562672777
                cashflow_from_operation = 2666464423603
                total_assets = 23048939945281
                longterm_debt = 12324531908724
                current_debt = 8428114074534
                sales = 11308423372686
                net_income = -407682308362
                operational_income = 43699451
                self.assertEqual(result['current_assets'],expected_current_assets)
                self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation)    
                self.assertEqual(result['total_assets'],total_assets)
                self.assertEqual(result['longterm_debt'],longterm_debt)
                self.assertEqual(result['current_debt'],current_debt)
                self.assertEqual(result['sales'],sales)
                self.assertEqual(result['operational_income'],operational_income)
                self.assertEqual(result['net_income'],net_income)

            else:
                expected_current_assets = 174697424
                cashflow_from_operation = 67031863
                total_assets = 339357244
                longterm_debt = 22522557
                current_debt = 69081510
                sales = 243771415
                net_income = -562967287220
                operational_income = 58886669
                self.assertEqual(result['current_assets'],expected_current_assets)
                self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation)    
                self.assertEqual(result['total_assets'],total_assets)
                self.assertEqual(result['longterm_debt'],longterm_debt)
                self.assertEqual(result['current_debt'],current_debt)
                self.assertEqual(result['sales'],sales)
                self.assertEqual(result['operational_income'],operational_income)
                self.assertEqual(result['net_income'],net_income)

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
                self.assertEqual(result['current_assets'],expected_current_assets*1000000)
                self.assertEqual(result['cashflow_from_operation'],cashflow_from_operation*1000000)    
                self.assertEqual(result['total_assets'],total_assets*1000000)
                self.assertEqual(result['longterm_debt'],longterm_debt*1000000)
                self.assertEqual(result['current_debt'],current_debt*1000000)
                self.assertEqual(result['sales'],sales*1000000)
                self.assertEqual(result['operational_income'],operational_income*1000000)
                self.assertEqual(result['net_income'],net_income*1000000)
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


    def test_return_company_report_data_list_hyundai(self):
        stock_code = 5380
        company_name="현대자동차"
        data_list = self.ddh.return_company_report_data_list(stock_code,company_name,YEARLY_REPORT)
        print(data_list)