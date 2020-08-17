from flask import (
    render_template, Blueprint, jsonify, 
    request, current_app, send_file
)
import logging

api = Blueprint('simple_api', __name__,)
logger = logging.getLogger(__name__)


@api.route('/', methods=["POST"])
def return_pdd_data_list():
    return jsonify({'result': "helloworld"})
