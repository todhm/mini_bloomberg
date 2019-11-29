import os 
import logging 
from log4mongo.handlers import BufferedMongoHandler,MongoHandler
import logstash


def log_factory(db_name):
    mongo_host = os.environ.get('MONGO_URI')
    logstash_host = os.environ.get("LOGSTASH_HOST")
    # handler = BufferedMongoHandler(
    #     host=mongo_host,
    #     database_name=db_name,
    #     collection='crawler_logs',
    #     buffer_early_flush_level=logging.ERROR
    # )
    handler = MongoHandler(
        host=mongo_host,
        database_name=db_name,
        collection='crawler_logs',
    )
    log = logging.getLogger('fpcrawler')
    log.setLevel(logging.DEBUG)
    log.handlers = []
    log.addHandler(handler)
    log.addHandler(
        logstash.LogstashHandler(logstash_host,5959,version=1)
    )
    return log,handler
