from typing import Dict
from pydantic import BaseModel
from enum import Enum
from celery.result import states


class Item(BaseModel):
    data: Dict = {}
    taskFunc: str


class CeleryChoices(str, Enum):
    pending = states.PENDING
    recieved = states.RECEIVED
    started = states.STARTED
    success = states.SUCCESS
    failure = states.FAILURE
    revoked = states.REVOKED
    rejected = states.REJECTED
    retry = states.RETRY
    ignored = states.IGNORED


class CeleryRespone(BaseModel):
    taskId: str
    

class CeleryStatusRespone(BaseModel):
    state: CeleryChoices
    