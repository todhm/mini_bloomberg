import os,sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
import logging
from application import create_app


app = create_app()


if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    
if __name__=="__main__":
    app.run()