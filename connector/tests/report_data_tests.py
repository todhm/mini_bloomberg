from datetime import datetime as dt
from datahandler import (
    reportdatahandler, 
    pipelinehandler,
    marketdatahandler
)
from .basetest import BaseTest

    
class ReportTest(BaseTest):
    def setUp(self):
        self.mdh = marketdatahandler.MarketDataHandler(dbName='testdb')
        super().setUp()
        
    def tearDown(self):
        super().tearDown()

    def test_report_data_inserts(self): 
        post_data_list = [
            {
                'company': '신한지주',
                'code': 55550,
                'company_category': '기타 금융업',
                'main_products': '금융지주회사',
                'register_date': '2001-09-10',
                'accounting_month': '12월',
                '대표자명': '조용병',
                'homepage': 'http://www.shinhangroup.com',
                'region': '서울특별시',
                'report_type': '사업보고서'
            },
            {
                'company': 'LG생활건강',
                'code': 51900,
                'company_category': '기타 화학제품 제조업',
                'main_products': '화장품,생활용품 제조,도매',
                'register_date': '2001-04-25',
                'accounting_month': '12월',
                '대표자명': '차 석 용',
                'homepage': 'http://www.lgcare.com',
                'region': '서울특별시',
                'report_type': '사업보고서'
            },
            {
                'company': 'LG화학',
                'code': 51910,
                'company_category': '기초 화학물질 제조업',
                'main_products': '유화/기능/합성수지,재생섬유소,산업재,리튬이온전지,평광판,PVC 제조,도매',
                'register_date': '2001-04-25',
                'accounting_month': '12월',
                '대표자명': '신학철',
                'homepage': 'http://www.lgchem.com',
                'region': '서울특별시',
                'report_type': '사업보고서'
            },
        ]
        reportdatahandler.insert_company_data_list(
            post_data_list, 
            db_name='testdb'
        )
        for data in post_data_list:
            data_list = list(self.db.report_data_list.find({'code': str(data['code'])}))
            failed_data_list = list(
                self.db.report_data_list.find(
                    {'code': data['code']}
                )
            )
            eq_data_list = list(
                self.db.eq_offer_list.find(
                    {'code': data['code']}
                )
            )
            print(failed_data_list)
            self.assertTrue(len(data_list) >= 10)


    def test_error_case(self):
        post_data_list = [
            {
                'company': '아프리카TV',
                'code': '41440',
                'report_type': '사업보고서'
            }
        ]
        reportdatahandler.insert_company_data_list(
            post_data_list, 
            db_name='testdb'
        )


    # 연결재무제표정보가있으면 연결재무제표 정보를 우선적으로가져오고 
    # 없으면 일반재무제표를 가져와서 알고리즘에 사용할  재무제표를 뽑아내는 함수
    def test_fetch_ml_data(self): 
        post_data_list = [
            {
                'company': '삼성전자',
                'code': 5930,
                'start_date': '20160101',
                'report_type': '사업보고서'
            }
        ]
        reportdatahandler.insert_company_data_list(
            post_data_list, 
            db_name='testdb'
        )
        startdate = dt.strptime('20200504', '%Y%m%d')
        enddate = dt.strptime('20200508', '%Y%m%d')
        self.mdh.save_api_market_data(startdate, enddate)
        pipeline_df = pipelinehandler.return_pipeline_data(
            '5930', 
            db_name='testdb'
        )
        self.assertTrue(len(pipeline_df) >= 1)        
