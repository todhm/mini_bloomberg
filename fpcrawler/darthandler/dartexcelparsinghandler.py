import re
import copy
from typing import List
from fp_common.fp_types import (
    BALANCE_RE,
    INCOME_RE,
    CASHFLOW_RE,
    NORMAL_FINANCIAL_STATEMENTS, 
    CONNECTED_FINANCIAL_STATEMENTS,
    petovski_balancesheet_dict,
    petovski_income_statement_dict,
    petovski_cash_dict,
    petovski_sub_income_dict
)
from .dartdataparsehandler import (
    parse_unit_string,
    return_remained_petovski_data

)
from utils import exception_utils
import xlrd
from xlrd.book import Book


class DartExcelParser(object):
    wb: Book
    sheet_name_list: List[str]
    final_result: dict

    def __init__(
        self, 
        link: str, 
        excel_link: str,
        fname: str, 
        code: str, 
        corp_name: str, 
        period_type: str,
        reg_date: str, 
        market_type: str,
        title: str,
        **kwargs
    ):
        self.excel_link = excel_link 
        self.link = link
        self.final_result = {
            'report_link': link,
            'table_link': excel_link,
            'code': code, 
            'corp_name': corp_name, 
            'period_type': period_type,
            'reg_date': reg_date, 
            'market_type': market_type,
            'title': title
        }
        self.fname = fname
        wb = xlrd.open_workbook(fname)
        self.wb = wb
        self.sheet_name_list = self.wb.sheet_names()

    def return_parsed_sheet_data(self, sheet_idx: int, petovski_dict):
        sheet = self.wb.sheet_by_index(sheet_idx)
        sheet_unit = self.parse_sheet_unit(sheet)
        sheet_data = self.parse_sheet_table(sheet, petovski_dict, sheet_unit)
        return sheet_data 

    def parse_cashflow_sheet(self, sheet_idx: int):
        result = self.return_parsed_sheet_data(
            sheet_idx, 
            petovski_cash_dict
        )
        return result

    def parse_incomestatement_sheet(self, sheet_idx):
        result = self.return_parsed_sheet_data(
            sheet_idx, 
            petovski_income_statement_dict
        )
        if result.get("sales") is not None and result['sales'] == 0:
            result.pop('sales')
        if 'extra_ordinary_profit' not in result:
            result['extra_ordinary_profit'] = 0 
        if 'extra_ordinary_loss' not in result:
            result['extra_ordinary_loss'] = 0
        if 'operational_income' not in result:
            try:
                operational_sales = None 
                for key in result:
                    sales_search = re.search(
                        petovski_sub_income_dict['operational_sales'],
                        key
                    )
                    if sales_search:
                        operational_sales = result[key]
                    cost_search = re.search(
                        petovski_sub_income_dict['operational_costs'],
                        key
                    )
                    if cost_search:
                        operational_costs = result[key]
                    if (
                        operational_sales is not None 
                        and operational_costs is not None
                    ):
                        result['operational_income'] = (
                            operational_sales - operational_costs
                        )
            except ValueError:
                pass

        if 'gross_profit' not in result:
            if result.get('sales'):
                sales = result['sales']
                original_dict = copy.deepcopy(result)
                for key in original_dict:
                    if re.search(
                        petovski_sub_income_dict['cost_of_goods_sold'],
                        key
                    ):
                        cogs = original_dict[key]
                        result['gross_profit'] = sales - cogs
        return result 

    def parse_sheet_table(self, sheet, re_dict, unit):
        data = {}
        for i in range(sheet.nrows):
            row = sheet.row_values(i)
            if (
                len(row) >= 2 and
                type(row[0]) is str and
                (type(row[1]) is int or type(row[1]) is float)
            ):
                is_pitovski_data = False
                for re_key in re_dict.keys():
                    re_val = re_dict[re_key]
                    if re.search(re_val, row[0]) and not data.get(re_key):
                        data[re_key] = row[1] * unit
                        is_pitovski_data = True
                        break
                if not is_pitovski_data:
                    row_key = row[0].strip().replace('.', '')
                    data[row_key] = row[1]
        return data

    def parse_sheet_unit(self, sheet):
        for i in range(sheet.nrows):
            row = sheet.row_values(i)
            for j in range(len(row)):
                if type(row[j]) is str and '단위' in row[j]:
                    return parse_unit_string(row[j])          
        return 1

    def return_final_sheet_table_data(
        self, 
        table_data: dict, 
        is_connected: bool = False
    ):
        balance_idx = (
            table_data['balance_sheet_connected'] 
            if is_connected else table_data['balance_sheet']
        )
        income_idx = (
            table_data['income_statement_connected'] 
            if is_connected else table_data['income_statement']
        )
        cash_idx = (
            table_data['cashflow_connected']
            if is_connected else table_data['cashflow']
        ) 
        balance_sheet_result = self.return_parsed_sheet_data(
            balance_idx,
            petovski_balancesheet_dict
        )
        incomestatement_result = self.parse_incomestatement_sheet(
            income_idx
        )
        cashflow_result = self.return_parsed_sheet_data(
            cash_idx, 
            petovski_cash_dict
        )
        if not cashflow_result.keys():
            depreciation_costs = None 
            tax_costs = None
            operational_income = incomestatement_result.get(
                'operational_income'
            )
            for key in incomestatement_result:
                depreciation_search = re.search(
                    petovski_sub_income_dict['depreciation_cost'],
                    key
                )
                if depreciation_search:
                    depreciation_costs = incomestatement_result[key]
                tax_search = re.search(
                    petovski_sub_income_dict['corporate_tax_cost'],
                    key
                )
                if tax_search:
                    tax_costs = incomestatement_result[key]
                if (
                    operational_income is not None and 
                    depreciation_costs is not None and 
                    tax_costs is not None
                ):
                    cashflow_result['cashflow_from_operation'] = (
                        operational_income + depreciation_costs - tax_costs
                    )
        
        cash_remained_keys = return_remained_petovski_data(
            cashflow_result,
            petovski_cash_dict
        )
        if cash_remained_keys:
            raise exception_utils.NotSufficientError("cashflow")
        balancesheet_remained_keys = return_remained_petovski_data(
            balance_sheet_result,
            petovski_balancesheet_dict
        )
        if balancesheet_remained_keys:
            raise exception_utils.NotSufficientError("balancesheet")
        incomestatement_remained_keys = return_remained_petovski_data(
            incomestatement_result,
            petovski_income_statement_dict
        )
        if incomestatement_remained_keys:
            raise exception_utils.NotSufficientError("income statement")
        result = {}
        result.update(cashflow_result)
        result.update(balance_sheet_result)
        result.update(incomestatement_result)
        result.update(self.final_result)
        return result

    def parse_excel_file_table(self):
        table_re_list = {
            'balance_sheet': BALANCE_RE,
            'income_statement': INCOME_RE,
            'cashflow': CASHFLOW_RE
        }
        data = {}
        for idx, sheet_name in enumerate(self.sheet_name_list):
            for (table_name, search_re) in table_re_list.items():
                if re.search(search_re, sheet_name) and '포괄' not in sheet_name:
                    is_connected = '연결' in sheet_name
                    table_key = (
                        table_name 
                        + '_connected' 
                        if is_connected else table_name
                    )
                    data[table_key] = idx

        if not data.get('income_statement'):
            for idx, sheet_name in enumerate(self.sheet_name_list):
                if re.search(table_re_list['income_statement'], sheet_name):
                    is_connected = '연결' in sheet_name
                    table_key = (
                        'income_statement' + '_connected' 
                        if is_connected else 'income_statement'
                    )
                    data[table_key] = idx
        return data

    def parse_excel_file_data(self):
        
        table_data = self.parse_excel_file_table()
        normal_table_must_list = [
            'balance_sheet', 
            'income_statement', 
            'cashflow'
        ]
        connected_table_must_list = [
            'balance_sheet_connected', 
            'income_statement_connected', 
            'cashflow_connected'
        ]
        is_normal_info = all([
            i in table_data.keys() 
            for i in normal_table_must_list
        ])
        is_connected_info = all([
            i in table_data.keys() for i in connected_table_must_list
        ])
        result = []
        if is_normal_info:
            final_result = self.return_final_sheet_table_data(
                table_data,
                is_connected=False
            )
            final_result['report_type'] = NORMAL_FINANCIAL_STATEMENTS
            result.append(final_result)
        if is_connected_info:
            final_result = self.return_final_sheet_table_data(
                table_data,
                is_connected=True
            )
            final_result['report_type'] = CONNECTED_FINANCIAL_STATEMENTS
            result.append(final_result)
        return result






