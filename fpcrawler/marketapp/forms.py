from typing import List
from pydantic import BaseModel


class Item(BaseModel):
    dateList: List[str] = []