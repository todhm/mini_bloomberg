from functools import lru_cache
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pipeline.routers import pipelineapp
from settings import Settings

@lru_cache()
def get_settings():
    return Settings()


async def get_token_header(x_token: str = Header(...)):
    if x_token != "fake-super-secret-token":
        raise HTTPException(status_code=400, detail="X-Token header invalid")


settings = get_settings()
app = FastAPI(debug=settings.DEBUG)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(pipelineapp)

