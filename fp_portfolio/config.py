import os 


class BaseConfig(object):
    app_name: str = "Pipeline API"
    DEBUG: bool = False
    MONGO_URI: str = os.environ.get("MONGO_URI")
    TESTING: bool = False


class TestConfig(BaseConfig):
    DEBUG: bool = True
    MONGODB_NAME: str = 'testdb'
    TESTING: bool = True
    

class DevelopmentConfig(BaseConfig):
    DEBUG: bool = True
    MONGODB_NAME = 'fp_data'
    

class ProuctionConfig(BaseConfig):
    DEBUG: bool = False
    MONGODB_NAME = 'fp_data'
    