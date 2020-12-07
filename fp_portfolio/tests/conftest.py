from functools import lru_cache

from fastapi.testclient import TestClient
import pytest
from mongoengine import connect, disconnect
from mongoengine.connection import _get_db
from main import create_application, get_settings
from settings import TestSettings


@lru_cache
def get_settings_override():
    return TestSettings()


@pytest.fixture(scope="module")
def test_app():
    # set up
    app, settings = create_application(get_settings_override)
    app.dependency_overrides[get_settings] = get_settings_override
    connect(     
        host=settings.MONGO_URI,
        db=settings.MONGODB_NAME
    )
    with TestClient(app) as test_client:
        yield test_client
    db = _get_db()
    db.client.drop_database(settings.MONGODB_NAME)
    disconnect()
