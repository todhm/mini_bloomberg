import os
from flask import Flask
from flask_cors import CORS
from flask_mongoengine import MongoEngine


db = MongoEngine()


def create_app(**config_overrides):

    # instantiate the app
    app = Flask("connector")

    CORS(app)
    # set config
    app_settings = os.getenv('APP_SETTINGS')
    app.config.from_object(app_settings)
    app.config.update(config_overrides)
    db.init_app(app)
    # set up extensions

    # register blueprints
    from simpleapi.views import api
    app.register_blueprint(api)

    from script.create_index import index_cli
    app.cli.add_command(index_cli)
    return app
