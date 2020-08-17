import os 
import json 
import unittest
from pymongo import MongoClient
from pendulum import Pendulum


class TaskInstance:
    @property
    def try_number(self):
        return 0

    def xcom_pull(self, task_ids="", key=""):
        return ['57baba8b1c72de18ba01cf8a']


class BaseTest(unittest.TestCase):

    def setUp(self):
        mongo_uri = os.environ.get("MONGO_URI")
        self.client = MongoClient(mongo_uri)
        self.db = self.client.testdb
        self.ti = TaskInstance()
        self.execution_date = Pendulum.now()
        self.ts = int(self.execution_date.timestamp())

    def tearDown(self):
        self.client.drop_database('testdb')




