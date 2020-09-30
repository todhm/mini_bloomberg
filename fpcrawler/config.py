import os




class TestConfig(object):
    """Base configuration."""
    WTF_CSRF_ENABLED = False
    CASH_RECEIPT_TEST = True
    TAX_INVOICE_TEST = True
    TESTING=True
    DEBUG=True
    SECRET_KEY="myprecious"
    ENV='testing'
    

class DevelopmentConfig(object):
    """Development configuration."""
    ENV = 'development'
    FLASK_ENV = 'development'
    WTF_CSRF_ENABLED = False
    CASH_RECEIPT_TEST = False
    TAX_INVOICE_TEST = False
    TESTING = False
    DEBUG = True
    SECRET_KEY = "myprecious"

class ProductionConfig(object):
    """Development configuration."""
    ENV='production'
    WTF_CSRF_ENABLED = False
    CASH_RECEIPT_TEST = False
    WTF_CSRF_ENABLED = False
    DEBUG=False
    TESTING=False
    SECRET_KEY="myprecious"
    ENV='production'
