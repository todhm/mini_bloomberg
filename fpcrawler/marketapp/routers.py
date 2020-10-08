from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import asyncio
from .datahandler import return_report_data
from .forms import Item

marketapp = APIRouter()


@marketapp.post("/return_market_data")
async def markets(item: Item):
    try:
        data_list = item.dateList
        request_list = [
            return_report_data(datestring) for datestring in data_list
        ]
        result = await asyncio.gather(*request_list, return_exceptions=True)
        for r in result:
            if isinstance(r, Exception):
                return JSONResponse(
                    {'message': str(r)},
                    status_code=400,
                )
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse(
            {'message': "Unexpected error " + str(e)},
            status_code=400,
        )
        