from datetime import datetime as dt
from pydantic.dataclasses import dataclass
from pymongo.collection import Collection
from dataclasses import asdict, field
from fp_types import (
    BUY_ML_LOW,
    NORMAL_FINANCIAL_STATEMENTS,
    CONNECTED_FINANCIAL_STATEMENTS
)
from typing import (
    Literal,
    List,
    Dict
)


@dataclass
class MlModel:
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

    @property
    def to_json(self):
        return asdict(self)
        
    def save(self, col: Collection):
        data = asdict(self)
        col.insert_one(data)


@dataclass
class Strategy:
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

    def save(self, col: Collection):
        data = asdict(self)
        col.insert_one(data)

    @property
    def to_json(self):
        return asdict(self)

