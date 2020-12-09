import logging

from flask import (
    Blueprint, jsonify
)
from mongoengine.connection import _get_db


logger = logging.getLogger(__name__)


portfolio_app = Blueprint('portfolio_app', __name__)


@portfolio_app.route("/", methods=['GET'])
def hello_world():
    db = _get_db()
    result = {
        'dbname': db.name
    }
    return jsonify(result)
