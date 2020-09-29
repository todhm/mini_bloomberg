import json
import unittest
import fp_types
from application import create_app


class ApiTest(unittest.TestCase):

    def create_app(self):
        app = create_app()
        app.config.from_object('config.TestConfig')
        return app

    def setUp(self):
        self.app = self.create_app()
        self.app_context = self.app.app_context()
        self.app_context.push()
        self.app.config['TESTING'] = True
        self.client = self.app.test_client()
    
    def test_report_data_api(self):
        post_data = {
            'company':  '삼성전자',
            'code':  5930,
            'start_date': '20160101'
        }
        response = self.client.post(
            '/company_report_data_list', 
            json=post_data
        )
        result = json.loads(response.data)
        print(result)
        success_list = result.get('success_list')
        self.assertTrue(len(success_list) >= 3)
        period_type_list = [x['period_type'] for x in success_list]
        self.assertTrue(fp_types.SEMINUAL_REPORT in period_type_list)
        self.assertTrue(fp_types.SEPTEMBER_REPORT in period_type_list)
        self.assertTrue(fp_types.MARCH_REPORT in period_type_list)
        self.assertTrue(fp_types.YEARLY_REPORT in period_type_list)

    def test_report_data_api_error_case(self):
        post_data =   {'company': '디에스티', 'code': 33430, 'company_category': '특수 목적용 기계 제조업', 'main_products': '몰리브덴 등', 'register_date': '19980525', 'accounting_month': '12월', 'ceo_name': '김윤기, 양성문(각자 대표이사)', 'homepage': 'http://www.korid.co.kr', 'region': '경상남도', 'start_date': '20160101'}
        response = self.client.post(
            '/company_report_data_list', 
            json=post_data
        )
        result = json.loads(response.data)
        success_list = result.get('success_list')
        self.assertTrue(len(success_list) >= 1)

    def test_report_api_third_case(self):
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
        response = self.client.post(
            '/company_report_data_list', 
            json=post_data
        )
        result = json.loads(response.data)
        success_list = result.get('success_list')
        self.assertTrue(len(success_list) >= 10)
        print(result.get('failed_list'))

        
    def test_report_data_second_error_case(self):
        post_data = {
                'company': '상신전자', 
                'code': 263810, 'company_category': '전자부품 제조업', 
                'main_products': '노이즈필터(48.81%), 코일(19.33%), 리액터(10.75%)', 
                'register_date': '20171016', 'accounting_month': '12월', 
                'ceo_name': '김승천', 'homepage': 'http://www.sangshin-e.com', 
                'region': '경상남도'
        } 
        response = self.client.post(
            '/company_report_data_list', 
            json=post_data
        )
        result = json.loads(response.data)
        success_list = result.get('success_list')
        self.assertTrue(len(success_list) >= 1)

    def test_report_data_api_with_quarter(self):
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
        response = self.client.post(
            '/company_report_data_list', 
            json=post_data
        )
        result = json.loads(response.data)
        success_list = result.get('success_list')
        self.assertTrue(len(success_list) >= 1)


    def test_report_async_error_case(self):
        post_data = {
            'code': 25750, 
            'company': '한솔홈데코', 
            'company_category': '제재 및 목재 가공업', 
            'main_products': 'PB,MDF,바닥재,강화,재생목재,LPM,제재목 제조,도소매,무역/임업,벌목관련 사업', 
            'start_date': '20160101', 
            'register_date': '20031104'
        }
        response = self.client.post(
            '/company_report_data_list', 
            json=post_data
        )
        result = json.loads(response.data)
        success_list = result.get('success_list')
        self.assertTrue(len(success_list) >= 1)


    def test_market_data_api(self):
        date_string_list = ['20200518', '20200504', '202005019']
        result = self.client.post(
            '/return_market_data', json={'dateList': date_string_list}
        )
        data_list = json.loads(result.data)
        self.assertEqual(len(data_list), 3)
        # print(data_list)
        self.assertTrue(len(data_list[0]) > 1000)
        first_ten_data = data_list[0][:10]
        for data in first_ten_data:
            self.assertEqual(type(data['Code']), str)
            self.assertEqual(type(data['Name']), str)
            self.assertEqual(type(data['Close']), float)
            self.assertEqual(type(data['Changes']), float)
            if not type(data['ChangesRatio']) is str:
                self.assertEqual(type(data['ChangesRatio']), float)
            else:
                self.assertEqual(data['ChangesRatio'], '')

            self.assertEqual(type(data['Volume']), float)
            self.assertEqual(type(data['Amount']), float)
            self.assertEqual(type(data['Open']), float)
            self.assertEqual(type(data['High']), float)
            self.assertEqual(type(data['Low']), float)
            self.assertEqual(type(data['Marcap']), float)
            self.assertEqual(type(data['MarcapRatio']), float)
            self.assertEqual(type(data['Stocks']), float)
            if data['ForeignShares']:
                self.assertEqual(type(data['ForeignShares']), float)
            else:
                self.assertEqual(data['ForeignShares'], '')

            if data['ForeignRatio']:
                self.assertEqual(type(data['ForeignRatio']), float)
            else:
                self.assertEqual(data['ForeignRatio'], '')
            self.assertEqual(type(data['Rank']), float)
            self.assertEqual(type(data['Date']), str)

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
    #     self.assertEqual(len(result), 1)
    #     self.assertTrue(len(result[0]) >= 200)
    #     for x in result[0]:
    #         self.assertEqual(type(x['company']), str)
    #         self.assertEqual(type(x['위험등급']), str)
    #         self.assertEqual(type(x['class']), str)
    #         self.assertEqual(type(x['total_amount']), int)
    #         self.assertEqual(type(x['stock_amount']), int)
    #         self.assertEqual(type(x['stock_ratio']), float)
    #         self.assertEqual(type(x['bond_amount']), int)
    #         self.assertEqual(type(x['bond_ratio']), float)
    #         self.assertEqual(type(x['security_contract_amount']), int)
    #         self.assertEqual(type(x['real_estate_amount']), int)
    #         self.assertEqual(type(x['real_estate_ratio']), float)
    #         self.assertEqual(type(x['goods_amount']), int)
    #         self.assertEqual(type(x['goods_ratio']), float)
    #         self.assertEqual(type(x['cash_amount']), int)
    #         self.assertEqual(type(x['cash_ratio']), float)
    #         self.assertEqual(type(x['standard_price']), float)
    #         self.assertEqual(type(x['one_month_return']), float)
    #         self.assertTrue(
    #             type(x['one_month_rank']) is int or x['one_month_rank'] is None
    #         ) 
    #         self.assertEqual(type(x['six_month_return']), float)
    #         self.assertTrue(
    #             type(x['six_month_rank']) is int or x['six_month_rank'] is None
    #         ) 
    #         self.assertEqual(type(x['one_year_return']), float)
    #         self.assertTrue(
    #             type(x['one_year_rank']) is int or x['one_year_rank'] is None
    #         )
    #         self.assertEqual(type(x['end_period_return']), float)
    #         self.assertTrue(
    #             type(x['end_period_rank']) is int or 
    #             x['end_period_rank'] is None
    #         )
    #         self.assertEqual(type(x['operational_cost']), float)
    #         self.assertEqual(type(x['sales_cost']), float)
    #         self.assertEqual(type(x['sales_comission']), float)
    #         self.assertEqual(type(x['sales_company']), str)
