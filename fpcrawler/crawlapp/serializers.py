from django.contrib.auth import get_user_model
from rest_framework import serializers
from django.contrib.auth.models import Group
from core.models import Task 
from datetime import datetime as dt
from django.utils.translation import ugettext_lazy as _
from fp_types import TASK_EXISTS,TASK_NOT_EXISTS
from celery.result import AsyncResult

class CrawlerSerializer(serializers.Serializer):
    stockCode = serializers.CharField(max_length=10)
    startDate = serializers.DateTimeField(allow_null=True)
    

    def validate(self,attrs):
        """Check if wait until same other task finished"""
        stockCode = attrs.get("stockCode")
        try:
            task = Task.objects.get(identifier=stockCode,complete=True).first()
        except Task.DoesNotExist:
            task = None 
        if task: 
            msg = _(TASK_EXISTS)
            raise serializers.ValidationError(msg, code=TASK_EXISTS)
        return attrs



class TaskSerializer(serializers.ModelSerializer):

    class Meta:
        model = Task
        fields = ('nk','complete','progress')


    # def validate(self,attrs):
    #     task = Task.objects.filter(nk=attrs.get('nk')).first()
    #     print(task,'a'*200)
    #     if not task: 
    #         msg = _(TASK_EXISTS)
    #         raise serializers.ValidationError(msg, code=TASK_EXISTS)
    #     return attrs 
            
    