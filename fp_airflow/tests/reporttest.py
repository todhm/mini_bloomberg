
from tests.basetests import BaseTest 
from dataclass_models.models import CompanyReport
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
            }
        ]
        data_list = [CompanyReport(**x).to_json for x in request_list]
        self.db.company_list.insert_many(data_list)
        handle_dart_jobs.insert_company_data_list(
            db_name='testdb', 
            execution_date=self.execution_date,
        )

        for request_data in request_list:
            data_list = list(
                self.db.report_data_list.find(
                    {'code': str(request_data['code'])}, {'code':1}
                )
            )
            print(request_data['code'])
            self.assertTrue(len(data_list) > 0)

    def test_reinsert_test(self):
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
        ]
        data_list = [CompanyReport(**x).to_json for x in request_list]
        self.db.company_list.insert_many(data_list)
        handle_dart_jobs.insert_company_data_list(
            db_name='testdb', 
            execution_date=self.execution_date,
        )
        report_counts = self.db.report_data_list.count()
        self.assertTrue(report_counts > 0)
        handle_dart_jobs.insert_continuous_company_report_list(
            db_name='testdb',
            execution_date=self.execution_date,
        )
        for request_data in request_list:
            data_list = list(
                self.db.report_data_list.find(
                    {'code': str(request_data['code'])}, {'code':1}
                )
            )
            print(request_data['code'])
            self.assertEqual(len(data_list), report_counts)