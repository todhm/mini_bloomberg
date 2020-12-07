from functools import lru_cache
from typing import Callable

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mongoengine import connect

from settings import Settings
from portfolio.routers import portfolio_app


@lru_cache()
def get_settings():
    return Settings()


async def get_token_header(x_token: str = Header(...)):
    if x_token != "fake-super-secret-token":
        raise HTTPException(status_code=400, detail="X-Token header invalid")


def create_application(setting_class: Callable) -> FastAPI:
    settings = setting_class()
    app = FastAPI(debug=settings.DEBUG)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(portfolio_app)
    return app, settings


app, settings = create_application(get_settings)


@app.on_event("startup")
def on_startup():
    connect(
        host=settings.MONGO_URI,
        db=settings.MONGODB_NAME
    )
