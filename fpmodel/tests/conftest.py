import pytest 
from pymongo import MongoClient
from pymongo.database import Database
from tests.test_app import settings
from tests.lr_test_app import settings as lrsettings
from tests.simulation_test_app import settings as stsettings


@pytest.fixture()
def mongodb() -> Database:
    client = MongoClient(settings.MONGO_URI)
    db = client[settings.MONGODB_NAME]
    yield db
    client.drop_database()
    client.close()
    

@pytest.fixture()
def longrunningmongo() -> Database:
    client = MongoClient(lrsettings.MONGO_URI)
    db = client[lrsettings.MONGODB_NAME]
    yield db
    db.ml_feature_list.drop()
    db.ml_model_result.drop()
    db.simulation_result.drop()
    client.close()
    

@pytest.fixture()
def simulationmongo() -> Database:
    client = MongoClient(stsettings.MONGO_URI)
    db = client[stsettings.MONGODB_NAME]
    yield db
    db.ml_model_result.drop()
    db.simulation_result.drop()
    db.portfolio.drop()
    db.recommendations.drop()
    client.close()