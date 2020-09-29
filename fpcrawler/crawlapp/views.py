from flask import (
    render_template, Blueprint, jsonify, 
    request, current_app, send_file
)
from darthandler.dartdatahandler import DartDataHandler
from marketdatahandler import marketdatahandler 
from funddatahandler.funddatahandler import return_fund_data_list
import logging
import requests

crawlapp_api = Blueprint('crawlapp', __name__,)
logger = logging.getLogger(__name__)


@crawlapp_api.route('/', methods=["GET"])
def hello_world():
  
    r = requests.get('http://ifconfig.me/ip')
    return 'You connected from IP address: ' + r.text

    
@crawlapp_api.route('/company_report_data_list', methods=["POST"])
def company_report_data_list():
    data = request.get_json()
    company = data.get('company')
    code = data.get('code')
    start_date = data.get('start_date', None)
    try:
        ddh = DartDataHandler()
        result = ddh.return_company_report_data_list(
            code, 
            company, 
            start_date=start_date
        )
        return jsonify(result)
    except Exception as e:
        result = {}
        result['errorMessage'] = str(e)
        return jsonify(result), 400


@crawlapp_api.route('/return_eq_api', methods=["POST"])
def return_eq_api():
    data = request.get_json()
    company = data.get('company')
    code = data.get('code')
    try:
        ddh = DartDataHandler()
        result = ddh.return_company_eq_offer_lists(code, company)
        return jsonify(result)
    except Exception as e: 
        result = {}
        result['errorMessage'] = str(e)
        return jsonify(result), 400


@crawlapp_api.route('/return_market_data', methods=["POST"])
def return_market_data():
    data = request.get_json()
    date_list = data.get('dateList')
    try:

        result = marketdatahandler.return_data_list(date_list)
        return jsonify(result)
    except Exception as e: 
        result = {}
        result['errorMessage'] = str(e)
        return jsonify(result), 400


@crawlapp_api.route('/return_fund_data_list', methods=["POST"])
def return_fund_data_list_api():
    data = request.get_json()
    data_list = data.get('dataList')
    try:

        result = return_fund_data_list(data_list)
        return jsonify(result)
    except Exception as e: 
        result = {}
        result['errorMessage'] = str(e)
        return jsonify(result), 400
