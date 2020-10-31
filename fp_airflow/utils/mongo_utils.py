import os
from pymongo import MongoClient


def create_mongodb_indexes(db_name: str):
    client = MongoClient(os.environ.get("MONGO_URI"))
    db = client[db_name]
    db.market_data.create_index('Code')
    db.market_data.create_index('Date')
    db.report_data_list.create_index('code')
    db.report_data_list.create_index('corp_name')
    db.report_data_list.create_index('report_link')
    db.report_data_list.create_index('reg_date')
    db.eq_offer_list.create_index('code')
    db.eq_offer_list.create_index('corp_name')
    db.eq_offer_list.create_index('report_link')
    db.eq_offer_list.create_index('reg_date')
    db.ml_feature_list.create_index('code')
    db.ml_feature_list.create_index('stock_date')
    db.ml_feature_list.create_index('report_type')
    client.close()
