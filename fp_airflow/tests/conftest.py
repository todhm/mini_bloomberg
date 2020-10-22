import os
import pytest 
from pymongo import MongoClient
from pymongo.database import Database


@pytest.fixture()
def mongodb() -> Database:
    client = MongoClient(os.environ.get("MONGO_URI"))
    db = client['testdb']
    yield db
    client.drop_database('testdb')
    client.close()