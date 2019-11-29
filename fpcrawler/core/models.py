from django.db import models
import datetime 
import hashlib 

from django.db import models 
from django.shortcuts import reverse 
from django.contrib.auth.models import AbstractUser
from django.conf import settings

#Create your models here.

class Task(models.Model):
    class Meta:
        db_table = 'task'
    nk = models.CharField(max_length=100,unique=True, db_index=True,primary_key=True)
    progress = models.IntegerField()
    name = models.CharField(max_length=200)
    identifier = models.CharField(max_length=200)
    complete = models.BooleanField(default=False)
    regDt = models.DateTimeField(auto_now_add=True)
    modDt = models.DateTimeField(auto_now=True)




#     def save(self, **kwargs):
#         if not self.identifier:
#             now = datetime.datetime.now()
#             secure_hash = hashlib.md5()
#             secure_hash.update(
#                 f'{now}'.encode(
#                     'utf-8'))
#             self.identifier = secure_hash.hexdigest()
#         super().save(**kwargs)
# # self.assertEqual(type(report_list[0]['link']),str)
# # self.assertEqual(type(report_list[0]['reg_date']),dt)
# # self.assertEqual(type(report_list[0]['market_type']),str)
# # self.assertEqual(type(report_list[0]['title']),str)
# # self.assertEqual(type(report_list[0]['corp_name']),str)

# # col_names = {'회사명':'company_name','종목코드':'code','업종':'company_category',
# #        '주요제품':'main_products','상장일':'register_date','결산월':'accounting_month','홈페이지':'homepage','지역':'region'}
# class KoreanCompanyList(models.Model):
#     class Meta:
#         db_table = 'korean_company_list'
#     code = models.CharField(max_length=100,unique=True,db_index=True,primary_key=True)
#     name = models.CharField(max_length=200,db_index=True)
#     sector = models.CharField(max_length=200)
#     main_products = models.CharField(max_length=200)
#     company_category = models.CharField(max_length=200)    
#     regDt = models.DateTimeField(auto_now_add=True)
#     modDt = models.DateTimeField(auto_now=True)


# class KoreanCompanyStockData(models.Model):
#     class Meta:
#         db_table = 'korean_stock_data'

#     code = models.ForeignKey(KoreanCompanyList, on_delete=models.CASCADE)
#     open = models.IntegerField()
#     high = models.IntegerField()
#     low = models.IntegerField()
#     close = models.IntegerField()
#     volume = models.BigIntegerField()
#     change = models.IntegerField()
#     date = models.DateField()


# data_object = {}
# data_object['code'] = code
# data_object['link'] = link
# data_object['reg_date'] = dt.strptime(date,"%Y-%m-%d")
# data_object['market_type'] = market
# data_object['title'] = title
# data_object['report_type'] = report_type
# data_object['reporter'] = reporter

# class KoreanReportList(models.Model):
#     class Meta:
#         db_table = 'korean_report_list'
#     YEARLY_REPORT="사업보고서"
#     QUARTER_REPORT="분기보고서"
#     nk = models.CharField(max_length=100,unique=True, db_index=True,primary_key=True)
#     STATUSES=((YEARLY_REPORT,YEARLY_REPORT),(QUARTER_REPORT,QUARTER_REPORT))
#     code = models.ForeignKey(KoreanCompanyList, on_delete=models.CASCADE)
#     link = models.URLField()
#     market_type = models.CharField()
#     title = models.CharField(max_length=200)
#     reg_date = models.DateField(max_length=200)
#     reporter = models.CharField(max_length=200)
#     report_type = models.CharField(chocies=STATUSES)
