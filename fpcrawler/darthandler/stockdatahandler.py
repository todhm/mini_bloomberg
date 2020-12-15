import FinanceDataReader as fdr
from twisted.internet import reactor                
from scrapy.crawler import CrawlerProcess, Crawler,CrawlerRunner
from scrapy.utils.project import get_project_settings
from bs4 import BeautifulSoup
import logging
import asyncio
import aiohttp
from datetime import timedelta,datetime as dt 
import sys 
import pandas as pd 
import requests
import json 
import os 
from math import ceil 
import re 
from utils.class_utils import DataHandlerClass
from utils.mongo_utils import bulk_mongo_inserts,bulk_first_mongo_inserts
from utils.api_utils import return_async_get
from utils.crawl_utils import get_crawl_soup,run_spider
from utils import get_kbd_diff,timestamp
from fp_common.fp_types import REQUEST_ERROR,BULK_DATA_FAILED,NO_DATA,NO_META_DATA\
                     ,CRAWL_PARSE_ERROR,BULK_DATA_WRITE,DAILY_STOCK_DATA_RESULT,\
                     DAILY_STOCK_DATA


logger = logging.getLogger(__name__)


def get_korean_stock_list(code,startdate='19990101'):
    try:
        stockcode = ''.zfill(6-len(code))+code
        df = fdr.DataReader(stockcode,startdate)
        return df.to_dict('record')
    except Exception as e: 
        raise ValueError(str(e))