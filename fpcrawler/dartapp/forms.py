from typing import Optional
from pydantic import BaseModel


class Item(BaseModel):
    company: str
    start_date: Optional[str] = None
    code: str
