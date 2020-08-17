from flask import url_for
import pandas as pd 
import asyncio
import os 
import json 
import time 
import unittest
from uuid import uuid4
from pymongo import MongoClient
from application import create_app, db


MONGO_TEST_DB_URI = os.environ.get("MONGO_DB_TEST_URI")

app = create_app(
    MONGODB_SETTINGS={
        'host': MONGO_TEST_DB_URI,
    },
    MONGO_URI=MONGO_TEST_DB_URI,
    TESTING=True,
    WTF_CSRF_ENABLED=False
)


class BaseTest(unittest.TestCase):
    """Include test cases on a given url"""
    def create_app(self):
        app.config.from_object('config.TestConfig')
        return app

    def setUp(self):
        self.app = self.create_app()
        self.app_context = self.app.app_context()
        self.app_context.push()
        self.app.config['TESTING'] = True
        self.client = self.app.test_client()
        self.mongo_client = MongoClient(os.environ.get('MONGO_URI'))
        self.db = self.mongo_client.testdb


    def tearDown(self):
        self.app_context.pop()
        self.mongo_client.drop_database('testdb')
        self.mongo_client.close()
        