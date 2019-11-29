from django.contrib.auth import get_user_model, login, logout # new
from rest_framework import generics, permissions, status, views, viewsets # new
from rest_framework.response import Response
from core.models import Task
from django.db.models import Q
from django.db import connections
from .serializers import CrawlerSerializer
from django.conf import settings
from utils.task_utils import launch_task
from fp_types import WORKER_ERROR,INVALID_TRANSACTION
import logging

logger = logging.getLogger(__name__)

class BasicWorkerApi(views.APIView):
    def make_rsp(self, response):
        if response.status_code == status.HTTP_200_OK:
            if not response.data:
                response.data = {}
                response.data['result'] = 'success'
            if type(response.data) != dict:
                response.data = {'result':"success",'data':response.data}
        return response 

    def launch_async_task(self,task_name,*args,identifier=False,**kwargs):
        data = {}
        db_name = settings.DB_NAME
        print(db_name,'a'*100)
        try:
            result = launch_task(
                task_name,
                db_name,
                *args,
                **kwargs
            )
        except Exception as e:
            data['errorMessage'] = str(e)
            logger.error(WOKRER_ERROR,extra=data)
            return self.make_rsp(Response(data,status=status.HTTP_400_BAD_REQUEST)) 
        try:
            if identifier:
                task = Task(nk=result.id,name=task_name,progress=0,identifier=identifier)
            else:
                task = Task(nk=result.id,name=task_name,progress=0)
            task.save()
        except Exception as e:
            data['errorMessage'] = str(e)
            logger.error(INVALID_TRANSACTION,extra=data)
            return self.make_rsp(Response(data,status=status.HTTP_400_BAD_REQUEST)) 
            
        data['taskId'] = task.nk
        data['result'] = 'success'
        return self.make_rsp(Response(data,status=status.HTTP_200_OK))



class CrawlBokMetaData(BasicWorkerApi):

    def post(self,*args,**kwargs):
        self.launch_async_task("prepare_bok_metadata")



class CrawlQuandlData(BasicWorkerApi):

    def post(self,*args,**kwargs):
        self.launch_async_task("insert_quandl_stats")

        



class CrawlDailyStock(BasicWorkerApi):

    serializer_class = CrawlerSerializer

    def post(self,*args,**kwargs):
        serializer = CrawlerSerializer(data=self.request.data)
        data = {}
        data['result'] = "failed"
        serializer_valid = serializer.is_valid(raise_exception=False)
        if serializer_valid \
            or (len(serializer.errors.keys()) ==1 and 'startDate' in serializer.errors):
            startDate = None if serializer.errors else self.request.data['startDate'][0]
            return self.launch_async_task("get_stock_daily_data",self.request.data['stockCode'],identifier=self.request.data['stockCode'][0],startDate=startDate)


        data = {}
        data['errorMessage'] = ' '.join([ ' '.join(serializer.errors[key]) for key in serializer.errors])
        return self.make_rsp(Response(data,status=status.HTTP_400_BAD_REQUEST)) 



class SaverReportData(BasicWorkerApi):

    def post(self,*args,**kwargs):
        return self.launch_async_task("insert_report_data",**self.request.data)



class CrawlDailyStock(BasicWorkerApi):

    serializer_class = CrawlerSerializer

    def post(self,*args,**kwargs):
        serializer = CrawlerSerializer(data=self.request.data)
        data = {}
        data['result'] = "failed"
        serializer_valid = serializer.is_valid(raise_exception=False)
        if serializer_valid \
            or (len(serializer.errors.keys()) ==1 and 'startDate' in serializer.errors):
            startDate = None if serializer.errors else self.request.data['startDate'][0]
            return self.launch_async_task("get_stock_daily_data",self.request.data['stockCode'],identifier=self.request.data['stockCode'][0],startDate=startDate)


        data = {}
        data['errorMessage'] = ' '.join([ ' '.join(serializer.errors[key]) for key in serializer.errors])
        return self.make_rsp(Response(data,status=status.HTTP_400_BAD_REQUEST)) 


