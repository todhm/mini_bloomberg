from application import db
from mongoengine import DateTimeField, StringField
import datetime


class MarketData(db.DynamicDocument):
    meta = {
        'collection': 'market_data',
        'indexes': ['Date', 'Code']
    }
    Date = DateTimeField(default=datetime.datetime.now)
    Code = StringField()


class ReportDataList(db.DynamicDocument):
    meta = {
        'collection': 'report_data_list',
        'indexes': ['code', 'reg_date', 'corp_name', 'report_link']
    }
    code = StringField()
    corp_name = StringField()
    report_link = StringField()
    reg_date = DateTimeField()


class FailedReportDataList(db.DynamicDocument):
    meta = {
        'collection': 'failed_report_list',
        'indexes': ['code', 'reg_date', 'corp_name', 'report_link']
    }
    code = StringField()
    corp_name = StringField()
    report_link = StringField()
    reg_date = DateTimeField()


class EqOfferList(db.DynamicDocument):
    meta = {
        'collection': 'eq_offer_list',
        'indexes': ['code', 'reg_date', 'corp_name', 'link']
    }
    code = StringField()
    corp_name = StringField()
    link = StringField()
    reg_date = DateTimeField()


class MlFeatureList(db.DynamicDocument):
    meta = {
        'collection': 'ml_feature_list',
        'indexes': ['code', 'report_type', 'stock_date']
    }
    