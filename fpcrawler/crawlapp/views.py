from django.shortcuts import render
from rest_framework import generics, permissions, status, views, viewsets # new
from core.models import Task
from .serializers import TaskSerializer
from celery.result import AsyncResult
from rest_framework.response import Response

# Create your views here.
class GetTaskView(viewsets.ReadOnlyModelViewSet):
    lookup_url_kwarg = "nk"
    queryset = Task.objects.all() 
    serializer_class = TaskSerializer


    def retrieve(self, request, *args, **kwargs):
        task = self.get_object()
        serializer = self.get_serializer(task)
        task_id = serializer.data.get('nk')
        res = AsyncResult(task_id)
        status = res.state
        data = serializer.data
        data['status'] = status 
        if status =="SUCCESS":
            data['result'] = "finished"
        elif status=="FAILURE":
            data['result'] = "failed"
        else:
            data['result'] = "running"
        if data['result'] == "failed" or data['result'] == "finished":
            task.complete=True
            task.progress=100
            task.save()
            data['complete'] = True 
            data['progress'] = 100
            data['data'] = res.result
        return Response(data)

    