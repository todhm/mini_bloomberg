import os
import pytest 
from pymongo import MongoClient
from pymongo.database import Database
from config import LongRunningTestSettings
from pendulum import Pendulum


@pytest.fixture()
def mongodb() -> Database:
    client = MongoClient(os.environ.get("MONGO_URI"))
    db = client['testdb']
    yield db
    client.drop_database('testdb')
    client.close()


@pytest.fixture()
def longrunningdb() -> Database:
    client = MongoClient(LongRunningTestSettings.MONGO_URI)
    db = client[LongRunningTestSettings.MONGODB_NAME]
    yield db
    db.ml_feature_list.drop()


@pytest.fixture()
def execution_date() -> Pendulum:
    execution_date = Pendulum.now()
    yield execution_date
    