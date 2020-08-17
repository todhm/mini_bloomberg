import os

MONGO_DB_URI = os.environ.get('MONGO_DB_URI')
MONGO_TEST_DB_URI = os.environ.get("MONGO_DB_TEST_URI")
MONGO_URI = os.environ.get("MONGO_URI")
TEST_DB_NAME = os.environ.get("MONGO_TEST_DB_NAME")
DB_NAME = os.environ.get("MONGO_DB_NAME")


class BaseConfig(object):
    pass


class TestConfig(BaseConfig):
    """Base configuration."""
    WTF_CSRF_ENABLED = False
    CASH_RECEIPT_TEST = True
    TAX_INVOICE_TEST = True
    MONGODB_SETTINGS = {
        'host': MONGO_TEST_DB_URI,
        'db': TEST_DB_NAME
    }
    MONGODB_HOST = MONGO_URI
    TESTING = True
    DEBUG = True
    SECRET_KEY = "myprecious"
    ENV = 'testing'
    

class DevelopmentConfig(BaseConfig):
    """Development configuration."""
    ENV = 'development'
    WTF_CSRF_ENABLED = False
    CASH_RECEIPT_TEST = False
    TAX_INVOICE_TEST = False
    TESTING = False
    DEBUG = True
    SECRET_KEY = "myprecious"
    MONGODB_SETTINGS = {
        'host': MONGO_DB_URI,
        'db': DB_NAME,
    }
    MONGODB_HOST = MONGO_URI
    ENV = 'development'


class ProductionConfig(BaseConfig):
    """Development configuration."""
    ENV = 'production'
    WTF_CSRF_ENABLED = False
    CASH_RECEIPT_TEST = False
    WTF_CSRF_ENABLED = False
    TAX_INVOICE_TEST = False
    DEBUG = False
    TESTING = False
    MONGODB_SETTINGS = {
        'host': MONGO_DB_URI,
        'db': TEST_DB_NAME
    }
    MONGODB_HOST = MONGO_URI
    SECRET_KEY = "myprecious"
    ENV = 'production'
