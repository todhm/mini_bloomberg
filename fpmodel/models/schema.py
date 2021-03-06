from datetime import datetime as dt
from marshmallow import (
    fields, Schema, validate
)
import typing


class PyDateTimeField(fields.DateTime):
    def deserialize(
        self,
        value: typing.Any,
        attr: str = None,
        data: typing.Mapping[str, typing.Any] = None,
        **kwargs
    ):
        if isinstance(value, dt):
            return value
        return super().deserialize(value, attr, data, **kwargs)


class MachineLearningSaveSchema(Schema):
    Close = fields.Integer()
    Marcap_lag = fields.Integer(validate=validate.Range(min=0))
    report_type = fields.Str()
    code = fields.Str()
    corp_name = fields.Str()
    june_accrual = fields.Float()
    june_atr = fields.Float()
    june_book_to_market = fields.Float(validate=validate.Range(min=0))
    june_book_value = fields.Integer()
    june_cashflow_from_operation = fields.Integer()
    june_cfo = fields.Float()
    june_current_assets = fields.Integer(validate=validate.Range(min=0))
    june_current_debt = fields.Integer(validate=validate.Range(min=0))
    june_current_ratio = fields.Float()
    june_delta_gmo = fields.Float()
    june_gmo = fields.Float()
    june_gross_profit = fields.Integer()
    june_leverage = fields.Float()
    june_leverage_diff = fields.Float()
    june_liquid = fields.Float()
    june_longterm_debt = fields.Integer()
    june_net_income = fields.Integer()
    june_operational_income = fields.Integer()
    june_roa = fields.Float()
    june_roa_diff = fields.Float()
    june_sales = fields.Integer(validate=validate.Range(min=0))
    june_total_assets = fields.Integer(validate=validate.Range(min=0))
    june_turonver = fields.Float()
    march_accrual = fields.Float()
    march_atr = fields.Float()
    march_book_to_market = fields.Float(validate=validate.Range(min=0))
    march_book_value = fields.Integer(validate=validate.Range(min=0))
    march_cashflow_from_operation = fields.Integer()
    march_cfo = fields.Float()
    march_current_assets = fields.Integer(validate=validate.Range(min=0))
    march_current_debt = fields.Integer(validate=validate.Range(min=0))
    march_current_ratio = fields.Float()
    march_delta_gmo = fields.Float()
    march_gmo = fields.Float()
    march_gross_profit = fields.Integer()
    march_leverage = fields.Float()
    march_leverage_diff = fields.Float()
    march_liquid = fields.Float()
    march_longterm_debt = fields.Integer(validate=validate.Range(min=0))
    march_net_income = fields.Integer()
    march_operational_income = fields.Integer()
    march_roa = fields.Float()
    march_roa_diff = fields.Float()
    march_sales = fields.Integer(validate=validate.Range(min=0))
    march_total_assets = fields.Integer(validate=validate.Range(min=0))
    march_turonver = fields.Float()
    reg_date = PyDateTimeField()
    yearly_report_link = fields.Str()
    september_report_link = fields.Str()
    march_report_link = fields.Str()
    june_report_link = fields.Str()
    yearly_reg_date = PyDateTimeField()
    june_reg_date = PyDateTimeField()
    september_reg_date = PyDateTimeField()
    march_reg_date = PyDateTimeField()
    return_10d = fields.Float()
    return_1d = fields.Float()
    return_5d = fields.Float()
    september_accrual = fields.Float()
    september_atr = fields.Float()
    september_book_to_market = fields.Float(validate=validate.Range(min=0))
    september_book_value = fields.Integer(validate=validate.Range(min=0))
    september_cashflow_from_operation = fields.Integer()
    september_cfo = fields.Float()
    september_current_assets = fields.Integer(validate=validate.Range(min=0))
    september_current_debt = fields.Integer(validate=validate.Range(min=0))
    september_current_ratio = fields.Float()
    september_delta_gmo = fields.Float()
    september_gmo = fields.Float()
    september_gross_profit = fields.Integer()
    september_leverage = fields.Float()
    september_leverage_diff = fields.Float()
    september_liquid = fields.Float()
    september_longterm_debt = fields.Integer(validate=validate.Range(min=0))
    september_net_income = fields.Integer()
    september_operational_income = fields.Integer()
    september_roa = fields.Float()
    september_roa_diff = fields.Float()
    september_sales = fields.Integer(validate=validate.Range(min=0))
    september_total_assets = fields.Integer(validate=validate.Range(min=0))
    september_turonver = fields.Float()
    stock_date = PyDateTimeField()
    yearly_accrual = fields.Float()
    yearly_atr = fields.Float()
    yearly_book_to_market = fields.Float(validate=validate.Range(min=0))
    yearly_book_value = fields.Integer(validate=validate.Range(min=0))
    yearly_cashflow_from_operation = fields.Integer()
    yearly_cfo = fields.Float()
    yearly_current_assets = fields.Integer(validate=validate.Range(min=0))
    yearly_current_debt = fields.Integer(validate=validate.Range(min=0))
    yearly_current_ratio = fields.Float()
    yearly_delta_gmo = fields.Float()
    yearly_gmo = fields.Float()
    yearly_gross_profit = fields.Integer()
    yearly_leverage = fields.Float()
    yearly_leverage_diff = fields.Float()
    yearly_liquid = fields.Float()
    yearly_longterm_debt = fields.Integer(validate=validate.Range(min=0))
    yearly_net_income = fields.Integer()
    yearly_operational_income = fields.Integer()
    yearly_roa = fields.Float()
    yearly_roa_diff = fields.Float()
    yearly_sales = fields.Integer(validate=validate.Range(min=0))
    yearly_total_assets = fields.Integer()
    yearly_turonver = fields.Float()