from mongoengine import (
    EmbeddedDocument,
    DynamicDocument
)
from mongoengine.fields import (
    StringField,
    IntField,
    FloatField,   
    ListField,
    EmbeddedDocumentField,
    DateTimeField,
    ReferenceField,
    DictField
)
from fp_common.fp_types import (
    CONNECTED_FINANCIAL_STATEMENTS,
    NORMAL_FINANCIAL_STATEMENTS
    
)

from fp_common.fp_utils.time_utils import get_now_time


class PossessStock(EmbeddedDocument):
    code = StringField(verbose_name='주식코드')
    price = FloatField(verbose_name='구매가격')
    count = IntField(verbose_name='구매수량')
    date = DateTimeField(verbose_name='구매날짜')


class RecommendBuyStock(EmbeddedDocument):
    code = StringField(verbose_name='주식코드')
    price = FloatField(verbose_name='현재가격')
    limit_price = FloatField(verbose_name='구매한계가격')
    count = IntField(verbose_name='구매수량')
    date = DateTimeField(verbose_name='구매날짜')


class RecommendSellStock(EmbeddedDocument):
    code = StringField(verbose_name='주식코드')
    buyprice = FloatField(verbose_name='주식구매가격')
    price = FloatField(verbose_name='현재가격')
    limit_price = FloatField(verbose_name='구매한계가격')
    count = IntField(verbose_name='구매수량')
    date = DateTimeField(verbose_name='구매날짜')


class SimulationResult(DynamicDocument):
    meta = {
        'collection': 'simulation_result',
    }
    model_name = StringField()
    strategy_name = StringField()
    report_type = StringField(
        default=NORMAL_FINANCIAL_STATEMENTS,
        choices=[
            NORMAL_FINANCIAL_STATEMENTS,
            CONNECTED_FINANCIAL_STATEMENTS
        ]
    )
    strategy_params = DictField(db_field='strategy_params')
    strategy_performance = DictField(db_field='strategy_performance')
    
    def __unicode__(self):
        return self.model_name


class Portfolio(DynamicDocument):
    meta = {
        'collection': 'portfolio',
    }
    initial_budget = FloatField()
    current_budget = FloatField()
    possess_list = ListField(
        EmbeddedDocumentField(PossessStock), 
        db_field="possess_list"
    )
    simulation_result = ReferenceField(
        SimulationResult, 
        required=False
    )
    reg_date = DateTimeField(default=get_now_time, db_field="reg_date")
    mod_date = DateTimeField(db_field="mod_date")


class Recommendations(DynamicDocument):
    meta = {
        'collection': 'recommendations',
    }
    portfolio = ReferenceField(
        Portfolio, 
        required=False
    )
    simulation_result = ReferenceField(
        SimulationResult, 
        required=False
    )
    buy_list = ListField(
        EmbeddedDocumentField(RecommendBuyStock), 
        db_field='buy_list'
    )
    sell_list = ListField(
        EmbeddedDocumentField(RecommendSellStock), 
        db_field='sell_list'
    )
    reg_date = DateTimeField(default=get_now_time, db_field="reg_date")

    