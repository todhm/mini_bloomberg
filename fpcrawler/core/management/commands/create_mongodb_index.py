from pymongo import MongoClient
from core import models 
from django.core.management.base import BaseCommand
from django.conf import settings
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
import os 

class Command(BaseCommand):
    help = "Create mongodb index"

   

    def handle(self, *args, **options):
        mongo_uri = os.environ.get('MONGO_URI')
        client = MongoClient(mongo_uri)
        print(settings)
        print(dir(settings))
        db_name = settings.DB_NAME
        db = client[db_name]
                        
        korean_company_list = db.korean_company_list                  
        daily_stock_price = db.daily_stock_price

        index_date = IndexModel([("date", DESCENDING)])
        index_stock = IndexModel([("code", ASCENDING)])      

        korean_company_list.create_index('code')
        daily_stock_price.create_indexes([index_date,index_stock])
        client.close()

            