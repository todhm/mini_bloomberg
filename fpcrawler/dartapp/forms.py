from typing import Optional, List
from pydantic import BaseModel


class Item(BaseModel):
    company: str
    start_date: Optional[str] = None
    code: str


class LinkDict(BaseModel):
    code: str
    link: str
    corp_name: str
    reg_date: str
    period_type: str
    market_type: str
    title: str
    

class LinkList(BaseModel):
    linkList: List[LinkDict] = []


class ReportDict(BaseModel):
    title: str
    code: str
    corp_name: str
    book_value: int
    cashflow_from_operation: int
    current_assets: int
    current_debt:  int  
    extra_ordinary_loss: int  
    extra_ordinary_profit: int
    gross_profit: int
    longterm_debt: int
    net_income: int
    operational_income: int 
    sales: int
    total_assets: int
    period_type: str
    market_type: str
    report_link: str
    reg_date: str
    report_type: str
    table_link: str

    
