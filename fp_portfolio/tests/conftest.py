import pytest
from mongoengine import connect, disconnect
from mongoengine.connection import _get_db

from application import create_app


@pytest.fixture(scope="module")
def test_app():
    # set up
    app = create_app('config.TestConfig')
    app_context = app.app_context()
    app_context.push()
    client = app.test_client()
    connect(     
        host=app.config.get('MONGO_URI'),
        db=app.config.get('MONGODB_NAME')
    )
    yield client
    db = _get_db()
    db.client.drop_database('testdb')
    disconnect()
    