from utils.log_utils import log_factory
from fp_types.errors import NO_DATA,REQUEST_ERROR
from pymongo import MongoClient
import os 
import asyncio
import aiohttp
from collections.abc import Iterable
import functools

class SingletonMeta(type):
    _instance_registry = {}    #Build an instance registry which tracks the different class objects
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instance_registry:    # check, if the class has already been instantiated
            cls._instance_registry[cls] = super().__call__(*args, **kwargs)
        return cls._instance_registry[cls]


class DataHandlerClass(metaclass=SingletonMeta): 


    def __init__(self,db_name='testdb'):
        self.db_name = db_name
        self.log,self.handler = log_factory(db_name)
        mongo_uri = os.environ.get('MONGO_URI')
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]

    

    def call_async_func(self,method_name,data_list,callback_func="",callback_args=False,use_callback=True,*args,**kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        async_list= []
        for data in data_list:
            method = getattr(self,method_name)
            is_single_args = isinstance(data,Iterable) and type(data) != str
            if is_single_args:
                task = loop.create_task(method(**data,**kwargs))
            else:
                task = loop.create_task(method(data,*args,**kwargs))

            if use_callback:
                if getattr(self,callback_func):
                    callback_method = getattr(self,callback_func)
                    if callback_args:
                        if is_single_args:
                            task.add_done_callback(
                            functools.partial(callback_method,data)
                            )
                        else:
                            task.add_done_callback(
                            functools.partial(callback_method,data,*args,**kwargs)
                            )
                    else:
                        task.add_done_callback(callback_method)
            async_list.append(task)
        all_results = asyncio.gather(*async_list)
        result_list = loop.run_until_complete(all_results)
        loop.close()
        return True


    def close(self):
        self.client.close()
