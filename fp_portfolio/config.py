import os 


class BaseConfig(object):
    SECRET_KEY: str = "flasksecretasdlkfjsd"
    app_name: str = "Pipeline API"
    DEBUG: bool = False
    MONGO_URI: str = os.environ.get("MONGO_URI")


class TestConfig(BaseConfig):
    DEBUG: bool = True
    MONGODB_NAME: str = 'testdb'
    TESTING: bool = True
    ENV: str = 'development'
    

class DevelopmentConfig(BaseConfig):
    DEBUG: bool = True
    TESTING: bool = True
    MONGODB_NAME = 'fp_data'
    ENV: str = 'development'
    

class ProuctionConfig(BaseConfig):
    DEBUG: bool = False
    TESTING: bool = False
    MONGODB_NAME = 'fp_data'
    ENV: str = 'production'
    