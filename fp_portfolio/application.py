from flask import Flask
from flask_admin import Admin
from flask_cors import CORS
from flask_admin.contrib.mongoengine import (
    ModelView
)

from portfolio.routers import portfolio_app
from portfolio.models import (
    Portfolio, 
    SimulationResult,
    Recommendations
)
from portfolio.admin import PortfolioView


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
    admin.add_view(PortfolioView(Portfolio, name='portfolio'))
    admin.add_view(ModelView(SimulationResult, name='simulation_result'))
    admin.add_view(ModelView(Recommendations, name='recommendation'))

    app.register_blueprint(portfolio_app)

    return app
