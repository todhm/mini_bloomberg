from flask import Flask
from flask_admin import Admin
from flask_cors import CORS
from portfolio.routers import portfolio_app


def create_app(config_object='config.DevelopmentConfig', **config_overrides):
    # instantiate the app
    app = Flask("fp_portfolio")
    admin = Admin()
    CORS(app)
    # set config
    app.config.from_object(config_object)
    app.config.update(config_overrides)

    # set up extensions
    admin.init_app(app)

    app.register_blueprint(portfolio_app)

    return app
