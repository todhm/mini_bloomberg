import os 


class BaseSettings:
    app_name: str = "Pipeline API"
    DEBUG: bool = False
    MONGO_URI: str = os.environ.get("MONGO_URI")
    CRAWLER_URI: str = os.environ.get("FPCRAWLER_URL")
    FPMODEL_URI: str = os.environ.get("FPMODEL_URL")
    MONGODB_NAME: str = 'fp_data'


class TestSettings(BaseSettings):
    DEBUG: bool = True
    MONGODB_NAME: str = 'testdb'
    

class LongRunningTestSettings(BaseSettings):
    DEBUG: bool = True
    MONGODB_NAME: str = 'longrunning'
    

class ProductionSettings(BaseSettings):
    DEBUG: bool = True
    MONGODB_NAME: str = 'fp_data'
    