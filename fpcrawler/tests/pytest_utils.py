from pymongo import MongoClient
import pytest
import os 

@pytest.fixture(scope='module')
def create_db():
    mongo_client = MongoClient(os.environ.get('MONGO_URI'))
    db = mongo_client.testdb
    yield db 
    mongo_client.drop_database('testdb')
    mongo_client.close()
