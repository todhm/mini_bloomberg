import unittest
from lxml.html import fromstring
from pymongo import MongoClient
from tests.basetest import BaseTest
from datetime import datetime as dt 
from datetime import timedelta 
from datahandler.stockdatahandler import StockDataHandler
from datahandler.dartdatahandler import DartDataHandler
from pandas.tseries.offsets import BDay
import requests
from bs4 import BeautifulSoup
from unittest.mock import patch 
from fp_types import DAILY_STOCK_DATA,DAILY_STOCK_DATA_RESULT,\
    BULK_DATA_FAILED,NO_DATA,DAILY_STOCK_DATA_RESULT,BULK_DATA_WRITE,\
    NORMAL_FINANCIAL_STATEMENTS,YEARLY_REPORT,QUARTER_REPORT,CONNECTED_FINANCIAL_STATEMENTS
from utils import timestamp
from utils import return_sync_get_soup
from utils.exception_utils import ReportLinkParseError,NotableError
from worker.tasks import insert_stock_data
from random import randint
import pandas as pd 
import pymongo
import os 


def create_company_list_data(company_list_col,**params):
    defaults = {
        'company_name':'에이스토리',
        'code':'241840',
        'company_category':'영화, 비디오물, 방송프로그램 제작 및 배급업',
        'main_products':'방송프로그램 제작',
        'register_date':'2019-07-19',
        'accounting_month':'12월',
        'homepage':'http://www.astory.co.kr',
        'region':'서울특별시'

    }
    defaults.update(params)
    company_list_col.insert_one(defaults)
    return defaults


def create_stock_data(stock_col,reg_dt = '2019-07-25',**params):
    date = dt.strptime(reg_dt,'%Y-%m-%d')
    defaults = {
        'date':date,
        'code':'241840',
        'open': 11750,
        'high': 12050,
        'low': 10600,
        'close': 10950,
        'volume': 1437601,
        'change': -0.06808510638297871
    }
    defaults.update(params)
    stock_col.insert_one(defaults)
    return defaults

    
class StockDataHandlerTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.stockcode = '018250'
        self.daily_stock_price = self.db.daily_stock_price
        self.crawler_logs = self.db.crawler_logs
        self.sdh = StockDataHandler('testdb')
        self.ts = timestamp()


    def check_daily_start_data(self,startDate):
        daily_stock_list = list(self.daily_stock_price.find())
        self.assertTrue(len(daily_stock_list)>10)
        for daily_stock in daily_stock_list:
            date = daily_stock['date']
            if startDate:
                self.assertTrue(date >= startDate)




    def test_prepare_comapny_list(self):
        self.sdh.prepare_stock_lists()
        self.sdh.close()
        data = list(self.db.korean_company_list.find())
        first_data = data[0]
        col_names = {'회사명':'company_name','종목코드':'code','업종':'company_category',
        '주요제품':'main_products','상장일':'register_date','결산월':'accounting_month','홈페이지':'homepage','지역':'region'}
        for key in col_names:
            self.assertTrue(col_names[key] in first_data)
        self.assertTrue(len(data)>2000)
        
    
    def test_insert_stock_data_without_previous(self):
        company_info = create_company_list_data(self.db.korean_company_list)
        self.sdh.save_stock_data(company_info)
        data = list(self.daily_stock_price.find({'code':company_info['code']}))
        self.assertTrue(len(data)>5)
        self.sdh.close()        

    def test_insert_stock_data_with_previous(self):
        company_info = create_company_list_data(self.db.korean_company_list)
        stock_data = create_stock_data(self.db.daily_stock_price,code=company_info['code'])
        self.sdh.save_stock_data(company_info)
        data = list(self.daily_stock_price.find({'code':company_info['code']},sort=[("date", pymongo.ASCENDING)]))
        self.assertTrue(len(data)>5)
        self.assertEqual(data[1]['date'].day,stock_data['date'].day+1)
        self.sdh.close()        


    
    @patch('worker.tasks.StockDataHandler.get_all_stock_list')
    def test_insert_process(self,stock_lists):
        company_info = create_company_list_data(self.db.korean_company_list)
        stock_lists.return_value = [company_info]
        result = insert_stock_data()
        data = list(self.daily_stock_price.find({'code':company_info['code']},sort=[("date", pymongo.ASCENDING)]))
        self.assertTrue(len(data)>5)





    
class FinanceTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.korean_company_list = self.db.korean_company_list
        self.ddh = DartDataHandler('testdb')
        self.company_report_list = self.db.company_report_list
        self.company_data_list = self.db.company_data_list

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

        


    def save_temp_object(self,link):
        code = randint(0,10000)
        date = '2019-04-18'
        reg_date = dt.strptime(date,"%Y-%m-%d")
        corp_name = '엘에이티'
        market = '주식회사'
        reporter = '김요환'
        title='분기보고서 2019.11.20'
        data_object = {}
        data_object['code'] = code
        data_object['link'] = link
        data_object['reg_date'] = reg_date
        data_object['corp_name'] = corp_name
        data_object['market_type'] = market
        data_object['title'] = title
        data_object['period_type'] = YEARLY_REPORT
        data_object['reporter'] = reporter
        self.company_report_list.insert_one(data_object)
        return code 

    def check_quarter_data(self,link):
        code = self.save_temp_object(link)
        self.ddh.save_company_all_financial_statements_data(code)
        data_list = list(self.db.company_data_list.find({"company_code":code}))
        data = data_list[0]
        before_year_asset = data['balancesheet_period_data_list'][1]['asset_sum']
        roa_son = data['net_income'] - data['special_income'] + data['special_loss']
        self.assertTrue(type(roa_son/before_year_asset),float)



    def check_onecolumn_data(self,link):
        code = self.save_temp_object(link)
        self.ddh.save_company_all_financial_statements_data(code)
        data_list = list(self.db.company_data_list.find({"company_code":code}))
        data = data_list[0]
        current_year_asset = data['asset_sum']

        roa_son = data['net_income'] - data['special_income'] + data['special_loss']

        long_term_debt = data['long_term_debt']
        current_asset = data['asset_current']
        if current_asset == 0 :
            current_asset += data['asset_current_sub1']
            current_asset += data['asset_current_sub2']
            current_asset += data['asset_current_sub3']

        current_year_liabilities = data['liability_current']
        if current_year_liabilities == 0 :
            current_year_liabilities += data['liability_current_sub1']
            current_year_liabilities += data['liability_current_sub2']
            current_year_liabilities += data['liability_current_sub3']
        current_year_gross_margin = data['gross_margin']
        current_year_gross_margin -= data['gross_loss']

        if current_year_gross_margin == 0:
            current_year_gross_margin = data['op_income'] + data['other_sales'] - data['other_costs']

        current_year_total_sales = data['sales'] 
        if current_year_total_sales ==0 :
            current_year_total_sales = data['sales2'] + data['other_sales']
        self.assertTrue(current_asset>0)
        self.assertTrue(current_year_gross_margin!=0)

    def check_two_period_data(self,link):
        code = self.save_temp_object(link)
        self.ddh.save_company_all_financial_statements_data(code)
        data_list = list(self.db.company_data_list.find({'company_code':code}))
        data = data_list[0]
        current_year_asset = data['asset_sum']
        before_year_asset = data['balancesheet_period_data_list'][1]['asset_sum']
        before_year_income_data = data['income_period_data_list'][1]

        current_net_income = data['net_income'] if data.get('net_income') and data.get('net_income') !=0 else data.get('net_loss') * -1
        roa_son = current_net_income - data['special_income'] + data['special_loss']
        last_year_special_income = before_year_income_data['special_income'] if before_year_income_data.get('special_income') else 0 
        last_year_special_loss = before_year_income_data['special_loss'] if before_year_income_data.get('special_loss') else 0 
        before_year_net_income = before_year_income_data['net_income'] if before_year_income_data.get('net_income',0) and before_year_income_data.get('net_income',0) !=0 else before_year_income_data.get('net_loss',0) * -1
        roa_last_year_son = before_year_net_income - last_year_special_income - last_year_special_loss

        average_total_year_asset = (current_year_asset+before_year_asset)/2
        long_term_debt = data['long_term_debt']
        leverage = long_term_debt / average_total_year_asset
        last_year_long_term_debt = data['balancesheet_period_data_list'][1]['asset_sum']
        average_total_lastyear_asset = (before_year_asset+last_year_long_term_debt)/2
        last_year_leverage = last_year_long_term_debt/average_total_lastyear_asset
        leverage_delta = leverage - last_year_leverage

        current_asset = data['asset_current']
        if current_asset == 0 :
            current_asset += data['asset_current_sub1']
            current_asset += data['asset_current_sub2']
            current_asset += data['asset_current_sub3']

        current_year_liabilities = data['liability_current']
        if current_year_liabilities == 0 :
            current_year_liabilities += data['liability_current_sub1']
            current_year_liabilities += data['liability_current_sub2']
            current_year_liabilities += data['liability_current_sub3']

        current_ratio = current_asset/current_year_liabilities
        last_year_current_asset = data['balancesheet_period_data_list'][1].get('asset_current')
        lb = data['balancesheet_period_data_list'][1]
        last_year_current_asset = last_year_current_asset if last_year_current_asset else 0
        if last_year_current_asset ==0:
            last_year_current_asset += lb['asset_current_sub1'] if lb.get('asset_current_sub1') else 0
            last_year_current_asset += lb['asset_current_sub2'] if lb.get('asset_current_sub2') else 0
            last_year_current_asset += lb['asset_current_sub3'] if lb.get('asset_current_sub3') else 0
        last_year_current_liabilities = lb.get('liability_current')
        last_year_current_liabilities = last_year_current_liabilities if last_year_current_liabilities else 0

        if last_year_current_liabilities ==0 :
            last_year_current_liabilities += lb['liability_current_sub1'] if lb.get('liability_current_sub1') else 0
            last_year_current_liabilities += lb['liability_current_sub2'] if lb.get('liability_current_sub2') else 0
            last_year_current_liabilities += lb['liability_current_sub3'] if lb.get('liability_current_sub3') else 0


        last_year_current_ratio = last_year_current_asset/last_year_current_liabilities
        liquid_delta = current_ratio - last_year_current_ratio

        common_stock_equity = data['common_stock_equity']
        lastyear_common_stock_equity = lb['common_stock_equity'] if lb.get('common_stock_equity') else 0 
        eq_offer = not (common_stock_equity - lastyear_common_stock_equity) >0

        cfo_son = data['op_cashflow']
        if cfo_son ==0:
            cfo_son = data['net_income'] + data['op_asset_debt_change'] + data['op_cash_add_cost']+data['op_none_cash_income']

        cfo = cfo_son/before_year_asset
        accurual_son = roa_son - cfo_son
        accurual  = accurual_son/before_year_asset
        roa = roa_son/before_year_asset

        current_year_gross_margin = data['gross_margin']
        current_year_gross_margin -= data['gross_loss']

        if current_year_gross_margin == 0:
            current_year_gross_margin = data['op_income'] + data['other_sales'] - data['other_costs']
        current_year_total_sales = data['sales'] 
        if current_year_total_sales ==0 :
            current_year_total_sales = data['sales2'] + data['other_sales'] +data['sales_finance']

        last_year_income_data = data['income_period_data_list'][1]
        current_year_gmo = current_year_gross_margin/current_year_total_sales
        last_year_gross_margin = last_year_income_data['gross_margin'] if last_year_income_data.get('gross_margin') else 0 
        last_year_gross_loss = last_year_income_data['gross_loss'] if last_year_income_data.get('gross_loss') else 0 
        last_year_gross_margin -= last_year_gross_loss

        if last_year_gross_margin ==0 :
            last_year_gross_margin = last_year_income_data['op_income'] + last_year_income_data.get('other_sales',0) - last_year_income_data.get('other_costs',0)


        last_year_total_sales = last_year_income_data['sales'] if last_year_income_data.get('sales') else 0 
        if last_year_total_sales ==0 :
            last_year_saels_finance = last_year_income_data.get('sales_finance',0)
            last_year_total_sales = last_year_income_data['sales2'] + last_year_income_data.get('other_sales',0)+last_year_saels_finance

        last_year_gmo = last_year_gross_margin / last_year_total_sales
        gmo_delta = current_year_gmo - last_year_gmo

        atr_this_year = current_year_total_sales/before_year_asset

        self.assertTrue(cfo_son>0)
        self.assertTrue(type(roa),float)
        self.assertTrue(type(cfo),float)
        self.assertTrue(type(accurual),float)
        self.assertTrue(type(leverage_delta),float)
        self.assertTrue(type(liquid_delta),float)
        self.assertTrue(type(gmo_delta),float)
        self.assertTrue(current_year_gross_margin!=0)
        self.assertTrue(last_year_gross_margin!=0)




    def check_annual_data(self,link):
        code = self.save_temp_object(link)
        self.ddh.save_company_all_financial_statements_data(code)
        data_list = list(self.db.company_data_list.find({'company_code':code}))
        data = data_list[0]
        current_year_asset = data['asset_sum']
        before_year_asset = data['balancesheet_period_data_list'][1]['asset_sum']
        before_year_before_asset = data['balancesheet_period_data_list'][2]['asset_sum']

        roa_son = data['net_income'] - data['special_income'] + data['special_loss']
        roa_last_year_son = data['net_income'] - data['special_income'] + data['special_loss']
        roa_last_year = roa_last_year_son/before_year_before_asset

        average_total_year_asset = (current_year_asset+before_year_asset)/2
        long_term_debt = data['long_term_debt']
        leverage = long_term_debt / average_total_year_asset
        last_year_long_term_debt = data['balancesheet_period_data_list'][1]['asset_sum']
        average_total_lastyear_asset = (before_year_asset+last_year_long_term_debt)/2
        last_year_leverage = last_year_long_term_debt/average_total_lastyear_asset
        leverage_delta = leverage - last_year_leverage

        current_asset = data['asset_current']
        if current_asset == 0 :
            current_asset += data['asset_current_sub1']
            current_asset += data['asset_current_sub2']
            current_asset += data['asset_current_sub3']

        current_year_liabilities = data['liability_current']
        if current_year_liabilities == 0 :
            current_year_liabilities += data['liability_current_sub1']
            current_year_liabilities += data['liability_current_sub2']
            current_year_liabilities += data['liability_current_sub3']

        current_ratio = current_asset/current_year_liabilities
        last_year_current_asset = data['balancesheet_period_data_list'][1].get('asset_current')
        lb = data['balancesheet_period_data_list'][1]
        last_year_current_asset = last_year_current_asset if last_year_current_asset else 0
        if last_year_current_asset ==0:
            last_year_current_asset += lb['asset_current_sub1'] if lb.get('asset_current_sub1') else 0
            last_year_current_asset += lb['asset_current_sub2'] if lb.get('asset_current_sub2') else 0
            last_year_current_asset += lb['asset_current_sub3'] if lb.get('asset_current_sub3') else 0
        last_year_current_liabilities = lb.get('liability_current')
        last_year_current_liabilities = last_year_current_liabilities if last_year_current_liabilities else 0

        if last_year_current_liabilities ==0 :
            last_year_current_liabilities += lb['liability_current_sub1'] if lb.get('liability_current_sub1') else 0
            last_year_current_liabilities += lb['liability_current_sub2'] if lb.get('liability_current_sub2') else 0
            last_year_current_liabilities += lb['liability_current_sub3'] if lb.get('liability_current_sub3') else 0


        last_year_current_ratio = last_year_current_asset/last_year_current_liabilities
        liquid_delta = current_ratio - last_year_current_ratio

        common_stock_equity = data['common_stock_equity']
        lastyear_common_stock_equity = lb['common_stock_equity'] if lb.get('common_stock_equity') else 0 
        eq_offer = not (common_stock_equity - lastyear_common_stock_equity) >0

        cfo_son = data['op_cashflow']
        cfo = cfo_son/before_year_asset
        accurual_son = roa_son - cfo_son
        accurual  = accurual_son/before_year_asset
        roa = roa_son/before_year_asset
        roa_diff = roa - roa_last_year

        current_year_gross_margin = data['gross_margin']
        current_year_gross_margin -= data['gross_loss']

        if current_year_gross_margin == 0:
            current_year_gross_margin = data['op_income'] + data['other_sales'] - data['other_costs']

        current_year_total_sales = data['sales'] 
        if current_year_total_sales ==0 :
            current_year_total_sales = data['sales2'] + data['other_sales']
        last_year_income_data = data['income_period_data_list'][1]
        current_year_gmo = current_year_gross_margin/current_year_total_sales
        last_year_gross_margin = last_year_income_data['gross_margin'] if last_year_income_data.get('gross_margin') else 0 
        last_year_gross_loss = last_year_income_data['gross_loss'] if last_year_income_data.get('gross_loss') else 0 
        last_year_gross_margin -= last_year_gross_loss

        if last_year_gross_margin ==0 :
            last_year_gross_margin = last_year_income_data['op_income'] + last_year_income_data['other_sales'] - last_year_income_data['other_costs']

        last_year_total_sales = last_year_income_data['sales'] if last_year_income_data.get('sales') else 0 
        if last_year_total_sales ==0 :
            last_year_total_sales = last_year_income_data['sales2'] + last_year_income_data['other_sales']

        last_year_gmo = last_year_gross_margin / last_year_total_sales
        gmo_delta = current_year_gmo - last_year_gmo

        atr_this_year = current_year_total_sales/before_year_asset
        atr_last_year = last_year_total_sales/before_year_before_asset
        atr_delta = atr_this_year - atr_last_year
        self.assertTrue(cfo_son>0)
        self.assertTrue(type(roa),float)
        self.assertTrue(type(cfo),float)
        self.assertTrue(type(accurual),float)
        self.assertTrue(type(roa_diff),float)
        self.assertTrue(type(leverage_delta),float)
        self.assertTrue(type(liquid_delta),float)
        self.assertTrue(type(gmo_delta),float)
        self.assertTrue(type(atr_delta),float)
        self.assertTrue(current_year_gross_margin!=0)
        self.assertTrue(last_year_gross_margin!=0)
    
    def test_with_large_numbers(self):
        stock_code = 5930
        company_name="삼성전자"
        self.ddh.save_company_report_list(stock_code,company_name)
        report_list = list(self.company_report_list.find())
        self.assertTrue(len(report_list)>=1)
        self.assertEqual(type(report_list[0]['link']),str)
        self.assertEqual(type(report_list[0]['reg_date']),dt)
        self.assertEqual(type(report_list[0]['market_type']),str)
        self.assertEqual(type(report_list[0]['title']),str)
        self.assertEqual(type(report_list[0]['corp_name']),str)

    def test_with_none_startdate(self):
        stock_code = 311060
        company_name="엘에이티"
        self.ddh.save_company_report_list(stock_code,company_name)
        report_list = list(self.company_report_list.find())
        self.assertTrue(len(report_list)>=1)
        self.assertEqual(type(report_list[0]['link']),str)
        self.assertEqual(type(report_list[0]['reg_date']),dt)
        self.assertEqual(type(report_list[0]['market_type']),str)
        self.assertEqual(type(report_list[0]['title']),str)
        self.assertEqual(type(report_list[0]['corp_name']),str)


    def test_check_data_sufficient_for_petovski(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190418000453'
        self.check_annual_data(link)

    def test_check_data_sufficient_for_petovski_seconds(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190418000453'
        code = self.save_temp_object(link)
        self.ddh.save_company_all_financial_statements_data(code)
        data_list = list(self.db.company_data_list.find())
        data = data_list[0]
        before_year_asset = data['balancesheet_period_data_list'][1]['asset_sum']
        roa_son = data['net_income'] - data['special_income'] + data['special_loss']
        cfo_son = data['op_cashflow']
        self.assertTrue(type(roa_son/before_year_asset),float)
        self.assertTrue(type(cfo_son/before_year_asset),float)



    def test_insert_financial_statements_onetime(self):    
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190418000453'
        code = self.save_temp_object(link)
        self.ddh.save_company_all_financial_statements_data(code)
        data_list = list(self.company_data_list.find({"report_type":NORMAL_FINANCIAL_STATEMENTS}))
        self.assertEqual(len(data_list),1)

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

    def test_parse_finance_link_soup(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190418000453'
        soup = return_sync_get_soup(link)
        link_data = self.ddh.parse_financial_section_link(soup)
        self.assertTrue(len(link_data.get('link_fs'))>0)
        self.assertTrue(len(link_data.get('link_connected_fs'))>0)


    def test_return_finance_report_table(self):
        link = 'http://dart.fss.or.kr/report/viewer.do?rcpNo=20190418000453&dcmNo=6704575&eleId=17&offset=255858&length=89703&dtd=dart3.xsd'
        financial_table_data = self.ddh.return_financial_report_table(link)
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
        financial_table_data = self.ddh.return_financial_report_table(link)
        self.check_finance_table_dictionary(financial_table_data)


    def test_parse_big_unit_finance_table_parser(self):
        link = 'http://dart.fss.or.kr/report/viewer.do?rcpNo=20050331002046&dcmNo=1049370&eleId=6415&offset=715476&length=306592&dtd=dart2.dtd'
        financial_table_data = self.ddh.return_financial_report_table(link)
        self.check_finance_table_dictionary(financial_table_data)
        for key in financial_table_data:
            self.assertEqual(financial_table_data[key]['unit'],1000000)


    def test_parse_eq_offer_list(self):
        stock_code = 5930
        company_name="삼성전자"
        eq_offer_list = self.ddh.return_company_eq_offer_lists(stock_code,company_name)
        self.assertEqual(len(eq_offer_list),1)
        
        
    def test_insert_financial_statements_multipletimes(self):
        code = 'abcd'
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190418000453'
        date = '2019-04-18'
        reg_date = dt.strptime(date,"%Y-%m-%d")
        corp_name = '엘에이티'

        title='분기보고서 2019.11.20'

        data_object = {}
        data_object['code'] = code
        data_object['link'] = link
        data_object['reg_date'] = reg_date
        data_object['corp_name'] = corp_name
        data_object['market_type'] = market
        data_object['title'] = title
        data_object['period_type'] = YEARLY_REPORT
        data_object['reporter'] = reporter
        self.company_report_list.insert_one(data_object)
        self.ddh.save_company_all_financial_statements_data(code)
        self.ddh.save_company_all_financial_statements_data(code)
        data_list = list(self.company_data_list.find({"report_type":NORMAL_FINANCIAL_STATEMENTS}))
        self.assertEqual(len(data_list),1)



    def test_insert_financial_statements_error_case(self):
        code = 'abcd'
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20011112000210'
        date = '2019-04-18'
        reg_date = dt.strptime(date,"%Y-%m-%d")
        corp_name = '엘에이티'
        market = '주식회사'
        reporter = '김요환'
        title='분기보고서 2019.11.20'
        data_object = {}
        data_object['code'] = code
        data_object['link'] = link
        data_object['reg_date'] = reg_date
        data_object['corp_name'] = corp_name
        data_object['market_type'] = market
        data_object['title'] = title
        data_object['period_type'] = YEARLY_REPORT
        data_object['reporter'] = reporter
        self.company_report_list.insert_one(data_object)
        self.ddh.save_company_all_financial_statements_data(code)
        data_list = list(self.company_data_list.find({"report_type":NORMAL_FINANCIAL_STATEMENTS}))
        self.assertEqual(len(data_list),1)





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



    def test_four_column_exists(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20040621000280'
        self.check_two_period_data(link)
        


    
    def test_check_income_table(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20170524000081'
        self.check_annual_data(link)


    def test_check_connected_data_jump_case(self):
        #테이블을 데이터가 있는 테이블을 건너뜀
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20170511003471'
        self.check_annual_data(link)
        data_list = list(self.db.company_data_list.find({"report_type":CONNECTED_FINANCIAL_STATEMENTS}))
        self.assertEqual(len(data_list),1)


    def test_timematch_error_case_where_period_list_exists_not_eixsts(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20151102000107'
        self.check_annual_data(link)
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20170331000172'
        self.check_annual_data(link)

    def test_linked_table_error_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20150514002062'
        self.check_annual_data(link)
        data_list = list(self.db.company_data_list.find({"report_type":CONNECTED_FINANCIAL_STATEMENTS}))
        self.assertEqual(len(data_list),1)


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

    def test_error_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190430000869'

    def test_connected_error_case(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20171114000945'
        self.check_two_period_data(link)
    def test_connected_none_table(self):
        link = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20000515000236'
        self.check_annual_data(link)
