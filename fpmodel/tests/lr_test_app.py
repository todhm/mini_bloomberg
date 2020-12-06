from fastapi.testclient import TestClient
from main import app, get_settings
from settings import LongRunningTestSettings
from functools import lru_cache


@lru_cache()
def get_longrunning_settings():
    return LongRunningTestSettings()


client = TestClient(app)
app.dependency_overrides[get_settings] = get_longrunning_settings
settings = get_longrunning_settings()
