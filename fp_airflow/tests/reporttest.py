
from tests.basetests import BaseTest 
from task_functions import handle_dart_jobs


class DartReportTest(BaseTest):

    def test_prepare_company_list(self):
        handle_dart_jobs.prepare_company_report_list(db_name='testdb')
        data_list = list(self.db.company_list.find())
        self.assertTrue(len(data_list) > 2000)

    def test_prepare_report_list(self):
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
            }, 
            {
                'company': '네오셈', 
                'code': 253590, 'company_category': '특수 목적용 기계 제조업', 
                'main_products': '반도체 검사장비', 'register_date': '20180404', 
                'accounting_month': '12월', 'ceo_name': '염동현', 
                'homepage': 'http://www.neosem.com', 'region': '경기도'
            }, 
            {
                'company': '상신전자', 
                'code': 263810, 'company_category': '전자부품 제조업', 
                'main_products': '노이즈필터(48.81%), 코일(19.33%), 리액터(10.75%)', 
                'register_date': '20171016', 'accounting_month': '12월', 
                'ceo_name': '김승천', 'homepage': 'http://www.sangshin-e.com', 
                'region': '경상남도'
            }, 
            {
                'company': '우리기술투자', 
                'code': 41190, 
                'company_category': '기타 금융업', 
                'main_products': '창업자에 대한 투자 및 융자', 'register_date': '20000609', 
                'accounting_month': '12월', 'ceo_name': '이정훈', 
                'homepage': 'http://www.wooricapital.co.kr', 
                'region': '서울특별시'
            }, 
            {
                'company': '이상네트웍스', 
                'code': 80010, 
                'company_category': '상품 중개업', 
                'main_products': '기업간전자상거래서비스', 
                'register_date': '20050930', 
                'accounting_month': '12월', 'ceo_name': '김기배', 
                'homepage': 'http://www.e-sang.net', 
                'region': '서울특별시'
            }, 
            {
                'company': '셀트리온헬스케어', 
                'code': 91990, 
                'company_category': '기타 전문 도매업', 
                'main_products': '바이오의약품 마케팅 및 판매', 
                'register_date': '20170728', 
                'accounting_month': '12월', 
                'ceo_name': '김형기', 
                'homepage': 'http://www.celltrionhealthcare.com/kr/index.do', 
                'region': '인천광역시'
            }
        ]
        for x in  request_list:
            x['start_date'] = '20160101'
        self.db.company_list.insert_many(request_list)
        handle_dart_jobs.insert_company_data_list(db_name='testdb')
        for request_data in request_list:
            data_list = list(
                self.db.company_report_list.find(
                    {'code': request_data['code']}, {}
                )
            )
            self.assertTrue(len(data_list) > 0)