from fastapi import APIRouter
from fastapi.responses import JSONResponse
from darthandler.dartdatahandler import DartDataHandler
from .forms import Item
import requests


dartapp = APIRouter()


@dartapp.get("/")
def hello_world():
    proxies = {'http': 'http:torproxy:9300'}
    r = requests.get('http://ifconfig.me/ip', proxies=proxies)
    return 'You connected from IP address: ' + r.text


@dartapp.post("/company_report_data_list")
async def company_report(item: Item):
    company = item.company
    code = item.code
    start_date = item.start_date
    try:
        ddh = DartDataHandler()
        result = await ddh.return_company_report_data_list(
            code, 
            company, 
            start_date=start_date
        )
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse(
            {'errorMessage': str(e)},
            status_code=400
        )


@dartapp.post("/return_eq_api")
async def return_eq_api(item: Item):
    company = item.company
    code = item.code
    try:
        ddh = DartDataHandler()
        result = ddh.return_company_eq_offer_lists(code, company)
        return JSONResponse(result)
    except Exception as e: 
        result = {}
        result['errorMessage'] = str(e)
        return JSONResponse(
            result,
            status_code=400
        )