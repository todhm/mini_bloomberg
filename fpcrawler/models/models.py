from datetime import datetime as dt
from marshmallow import (
    fields, Schema, validate
)
import typing


class ReportSchema(Schema):
    code = fields.Str()
    corp_name = fields.Str()
    period_type = fields.Str()
    market_type = fields.Str()
    book_value = fields.Integer()
    cashflow_from_operation = fields.Integer()
    current_assets = fields.Integer(validate=validate.Range(min=0))
    current_debt = fields.Integer(validate=validate.Range(min=0))
    gross_profit = fields.Integer()
    longterm_debt = fields.Integer()
    net_income = fields.Integer()
    operational_income = fields.Integer()
    sales = fields.Integer(validate=validate.Range(min=0))
    total_assets = fields.Integer(validate=validate.Range(min=0))
    reg_date = fields.Str()
    report_link = fields.Str()
    