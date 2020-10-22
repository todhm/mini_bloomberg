from fastapi.testclient import TestClient
from main import app, get_settings
from settings import TestSettings
from functools import lru_cache


@lru_cache
def get_settings_override():
    return TestSettings()


client = TestClient(app)
app.dependency_overrides[get_settings] = get_settings_override
settings = get_settings_override()
