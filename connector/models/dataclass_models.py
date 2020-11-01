from datetime import datetime as dt
from pydantic.dataclasses import dataclass
from dataclasses import asdict, field
from fp_types import (
    BUY_ML_LOW
)
from typing import (
    Literal,
    List,
    Dict
)


@dataclass
class MlModel:
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


@dataclass
class Strategy:
    reg_date: dt = dt.now()
    model_name: str = ""
    strategy_name: Literal[
        BUY_ML_LOW
    ] = ""
    strategy_params: Dict = field(default_factory=dict)
    strategy_performance: Dict = field(default_factory=dict)
