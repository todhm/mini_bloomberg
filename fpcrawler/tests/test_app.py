from fastapi.testclient import TestClient
from main import app, get_settings
from settings import Settings


def get_settings_override():
    return Settings(admin_email="testing_admin@example.com")


client = TestClient(app)
app.dependency_overrides[get_settings] = get_settings_override
