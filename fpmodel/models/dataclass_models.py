from datetime import datetime as dt
from dataclasses import asdict, field
from typing import (
    Literal,
    List,
    Dict,
    Optional
)

from bson.objectid import ObjectId
from pydantic.dataclasses import dataclass
from pymongo.collection import Collection

from fp_common.fp_types import (
    BUY_ML_LOW,
    NORMAL_FINANCIAL_STATEMENTS,
    CONNECTED_FINANCIAL_STATEMENTS
)
from fp_common.fp_utils.time_utils import get_now_time


class PydanticObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, ObjectId):
            raise TypeError('ObjectId required')
        return v

@dataclass
class BaseModel:
    
    @property
    def to_json(self):
        return asdict(self)
        
    def save(self, col: Collection):
        data = asdict(self)
        col.insert_one(data)


@dataclass
class MlModel(BaseModel):
    report_type: Literal[
        NORMAL_FINANCIAL_STATEMENTS,
        CONNECTED_FINANCIAL_STATEMENTS
    ]
    reg_date: dt = dt.now()
    model_name: str = ""
    model_features: List = field(default_factory=list)
    train_code_list: List = field(default_factory=list)
    test_code_list: List = field(default_factory=list)
    model_params: Dict = field(default_factory=dict)
    model_performance: Dict = field(default_factory=dict)


@dataclass
class Simulation(BaseModel):
    reg_date: dt = dt.now()
    model_name: str = ""
    strategy_name: Literal[
        BUY_ML_LOW
    ] = BUY_ML_LOW
    strategy_params: Dict = field(default_factory=dict)
    final_possess_products: Dict = field(default_factory=dict)
    report_type: Literal[
        NORMAL_FINANCIAL_STATEMENTS,
        CONNECTED_FINANCIAL_STATEMENTS
    ] = NORMAL_FINANCIAL_STATEMENTS
    strategy_performance: Dict = field(default_factory=dict)

  
@dataclass
class StockPossessData:
    date: dt = dt.now()
    code: str = ""
    count: int = 0
    price: int = 0
    
    @property
    def to_json(self):
        return asdict(self)


@dataclass
class RecommendStock:
    date: dt = dt.now()
    code: str = ""
    count: int = 0
    price: float = 0
    limit_price: float = 0

    @property
    def to_json(self):
        return asdict(self)


@dataclass
class RecommendBuyStock:
    date: dt = dt.now()
    code: str = ""
    count: int = 0
    price: float = 0
    limit_price: float = 0

    @property
    def to_json(self):
        return asdict(self)


@dataclass
class RecommendSellStock:
    date: dt = dt.now()
    code: str = ""
    count: int = 0
    price: float = 0
    buyprice: float = 0
    limit_price: float = 0

    @property
    def to_json(self):
        return asdict(self)


@dataclass
class Portfolio(BaseModel):
    _id: Optional[PydanticObjectId] = None
    initial_budget: float = 0.0
    current_budget: float = 0.0
    possess_list: List[StockPossessData] = field(default_factory=list)
    simulation_result: Optional[PydanticObjectId] = None
    reg_date: dt = get_now_time()


@dataclass
class Recommendation(BaseModel):
    simulation_result: Optional[PydanticObjectId] = None
    portfolio: Optional[PydanticObjectId] = None
    buy_list: List[RecommendBuyStock] = field(default_factory=list)
    sell_list: List[RecommendSellStock] = field(default_factory=list)
    date: Optional[dt] = None
    reg_date: dt = get_now_time()
