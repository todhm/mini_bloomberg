from flask import render_template, Blueprint, jsonify, \
    request, current_app,send_file
from datahandler.dartdatahandler import DartDataHandler
import logging


crawlapp_api = Blueprint('crawlapp', __name__,)
logger = logging.getLogger(__name__)

@crawlapp_api.route('/',methods=["GET"])
def hello_world():
    result = {'result':'Hello World'}
    return jsonify(result)


@crawlapp_api.route('/company_report_data_list', methods=["POST"])
def company_report_data_list():
    data = request.get_json()
    company = data.get('company')
    code = data.get('code')
    report_type = data.get('report_type')
    try:
        ddh = DartDataHandler()
        result = ddh.return_company_report_data_list(
            code, company, report_type
        )
        return jsonify(result)
    except Exception as e: 
        result = {}
        result['errorMessage'] = str(e)
        return jsonify(result), 400


@crawlapp_api.route('/return_eq_api',methods=["POST"])
def return_eq_api():
    data = request.get_json()
    company = data.get('company')
    code = data.get('code')
    try:
        ddh = DartDataHandler()
        result = ddh.return_company_eq_offer_lists(code,company)
        return jsonify(result)
    except Exception as e: 
        result = {}
        result['errorMessage'] = str(e)
        return jsonify(result),400


