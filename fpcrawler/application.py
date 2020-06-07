import os

from flask import Flask
from flask_cors import CORS


def create_app(**config_overrides):

    # instantiate the app
    app = Flask("fpcrawler")

    CORS(app)
    # set config
    app_settings = os.getenv('APP_SETTINGS')
    app.config.from_object(app_settings)
    app.config.update(config_overrides)
    # set up extensions

    # register blueprints
    from crawlapp.views import crawlapp_api

    app.register_blueprint(crawlapp_api)
    return app
