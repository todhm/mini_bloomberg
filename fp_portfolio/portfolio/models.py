from mongoengine import EmbeddedDocument
from mongoengine.fields import (
    StringField,
    IntField,
    FloatField,   
)


class Portfolio(EmbeddedDocument):

    initial_budget = IntField()
    