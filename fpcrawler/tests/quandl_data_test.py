import pytest
import sys 
from pymongo import MongoClient
from datahandler.quandldatahandler import QuandlDataHandler
from tests.pytest_utils import create_db
import os 

@pytest.fixture(scope='module')
def code_list():
    code_list = ['LBMA/GOLD','SHFE/AUV2013']
    return code_list

@pytest.fixture(scope='module')
def columns_data():
    columns_data = [
        'id',
        'dataset_code',
        'database_code',
        'name',
        'description',
        'refreshed_at',
        'newest_available_date',
        'oldest_available_date',
        'column_names',
        'frequency',
        'type',
        'premium'
    ]
    return columns_data


def prepare_col(db):
    quandl_meta = db.quandl_meta
    quandl_stats = db.quandl_stats
    return quandl_meta,quandl_stats


def insert_meta_data(code_list,columns_data,qdh):
    qdh.insert_metadata(code_list)

    
def check_inserted_quandl_data(code_list,quandl_meta,quandl_stats):
    for code in code_list:
        temp_code_list = code.split('/')
        dataset_code = temp_code_list[1]
        database_code = temp_code_list[0]
        cur = list(quandl_meta.find({
            "dataset_code":dataset_code,
            "database_code":database_code}))[0]
        columns = cur['column_names']
        data_id = cur['id']
        data_list = list(quandl_stats.find({"id":data_id}))
        assert len(data_list) >0
        for stat in data_list:
            for col in columns:
                col = col.replace('.','')
                assert col in stat 

def test_insert_metadata(create_db,code_list,columns_data):
    db = create_db
    quandl_meta,quandl_stats = prepare_col(db)
    qdh = QuandlDataHandler('testdb')
    insert_meta_data(code_list,columns_data,qdh)
    qdh.insert_metadata(code_list)
    list_counts = quandl_meta.count()
    assert(list_counts == len(code_list))
    find_cur = quandl_meta.find()
    for code in code_list:
        temp_code_list = code.split('/')
        dataset_code = temp_code_list[1]
        database_code = temp_code_list[0]
        cur = list(quandl_meta.find({
            "dataset_code":dataset_code,
            "database_code":database_code}))[0]
        for key in columns_data:
            assert key in cur 
        
def test_insert_first_data(create_db,code_list,columns_data):
    db = create_db
    quandl_meta,quandl_stats = prepare_col(db)
    qdh = QuandlDataHandler('testdb')
    insert_meta_data(code_list,columns_data,qdh)
    qdh.insert_first_stats(code_list)
    check_inserted_quandl_data(code_list,quandl_meta,quandl_stats)

        
def test_insert_data(create_db,code_list,columns_data):
    db = create_db
    quandl_meta,quandl_stats = prepare_col(db)
    qdh = QuandlDataHandler('testdb')
    insert_meta_data(code_list,columns_data,qdh)
    qdh.insert_first_stats(code_list)
    qdh.insert_stats(code_list)
    check_inserted_quandl_data(code_list,quandl_meta,quandl_stats)
