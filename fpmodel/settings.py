import os 
from pydantic import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Pipeline API"
    DEBUG: bool = False
    MONGO_URI: str = os.environ.get("MONGO_URI")
    MONGODB_NAME: str = 'fp_data'


class TestSettings(Settings):
    DEBUG: bool = True
    MONGODB_NAME: str = 'testdb'
    

class LongRunningTestSettings(Settings):
    DEBUG: bool = True
    MONGODB_NAME: str = 'longrunning'
    