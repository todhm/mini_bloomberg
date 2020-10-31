from typing import List
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from darthandler.dartdatahandler import DartDataHandler
from .forms import (
    Item, LinkList,  ReportDict
)
import requests
import logging

logger = logging.getLogger(__name__)


dartapp = APIRouter()


@dartapp.get("/")
def hello_world():
    r = requests.get('http://ifconfig.me/ip')
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


@dartapp.post("/links", response_model=LinkList)
async def report_links(item: Item):
    company = item.company
    code = item.code
    start_date = item.start_date
    try:
        ddh = DartDataHandler()
        result = await ddh.return_company_link_list(
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


@dartapp.post("/report", response_model=List[ReportDict])
async def crawl_report(link_list: LinkList):
    try:
        ddh = DartDataHandler()
        data_dict = link_list.dict()
        link_list = data_dict['linkList']
        result = await ddh.return_multiple_link_results(
            link_list
        )
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse(
            {'errorMessage': str(e)},
            status_code=400
        )