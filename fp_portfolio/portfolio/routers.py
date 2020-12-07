import logging

from mongoengine import get_db
from fastapi import APIRouter
from fastapi.responses import JSONResponse


logger = logging.getLogger(__name__)


portfolio_app = APIRouter()


@portfolio_app.get("/")
async def hello_world():
    db = get_db()
    result = {
        'dbname': db.name
    }
    return JSONResponse(result)
