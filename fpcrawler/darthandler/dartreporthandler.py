import lxml.html as LH
import re
from bs4 import BeautifulSoup
from .dartdataparsehandler import (
    return_remained_petovski_data,
    parse_row_data_with_re,
    parse_cashflow_table,
    parse_incomestatement_table,
    parse_balancesheet_table,
    parse_last_year_row,
    fillin_insufficient_balance_data,
    return_financial_report_table
)
from .dartdriverparsinghandler import return_driver_report_data
from fp_common.fp_types import (
    petovski_balancesheet_dict,
    petovski_cash_dict,
    petovski_income_statement_dict,
    petovski_sub_income_dict,
    TABLE_CASE_FIRST,
    TABLE_CASE_WITH_NOTE,
    TABLE_CASE_SECOND,
    TABLE_CASE_THIRD
)
from utils import exception_utils
from scrapy.http import HtmlResponse
import logging
from copy import deepcopy

logger = logging.getLogger(__name__)


class DartReportHandler(object):

    def __init__(
        self, 
        link: str, 
        table_link: str,
        soup: BeautifulSoup, 
        code: str, 
        corp_name: str, 
        report_type: str,
        period_type: str,
        reg_date: str, 
        market_type: str,
        title: str
    ):
        self.table_link = table_link 
        self.link = link
        self.result = {
            'report_link': link,
            'table_link': table_link,
            'code': code, 
            'corp_name': corp_name, 
            'report_type': report_type, 
            'period_type': period_type,
            'reg_date': reg_date, 
            'market_type': market_type,
            'title': title
        }
        self.soup = soup

    def fill_insufficient_petovski_data(
            self, 
            data_dict: dict,
            root: HtmlResponse, 
            unit: int, 
    ):
        remained_keys = return_remained_petovski_data(self.result, data_dict)
        for key in remained_keys:
            try:
                value = parse_row_data_with_re(
                    root, data_dict[key], unit, data_key=key
                )
                self.result[key] = value
            except Exception:
                pass

    def parse_report_link(
        self,
        is_connected_inside_normal: bool = False,
    ) -> dict:
        table_dict = return_financial_report_table(
            self.table_link,
            self.soup,
            is_connected_inside_normal=is_connected_inside_normal
        )
        if table_dict.get('cashflow') and table_dict['cashflow']['style'] in [
            TABLE_CASE_FIRST, TABLE_CASE_WITH_NOTE
        ]:
            try:
                cashflow_result = parse_cashflow_table(
                    table_dict['cashflow']['table'],
                    table_dict['cashflow']['unit'],
                    table_dict['cashflow']['style']
                )
                self.result.update(cashflow_result)
            except exception_utils.TableStyleError:
                message = (
                    f"Unexpected cashflow table {self.link} {self.table_link}"
                )
                logger.error(message)
                raise exception_utils.ExpectedError(message)
        try:
            if (
                table_dict.get('income_statement') and 
                table_dict['income_statement']['style'] in 
                [
                    TABLE_CASE_FIRST, TABLE_CASE_WITH_NOTE
                ]
            ):
                incomestatement_result = parse_incomestatement_table(
                    table_dict['income_statement']['table'], 
                    table_dict['income_statement']['unit'],
                    table_dict['income_statement']['style']
                )
                self.result.update(incomestatement_result)
        except exception_utils.TableStyleError:
            message = (
                "Unexpected income statement table"
                f"{self.link} {self.table_link}"
            )
            logger.error(message)
            raise exception_utils.ExpectedError(message)
        try:
            if (
                table_dict.get('balance_sheet') and 
                table_dict['balance_sheet']['style'] in [
                    TABLE_CASE_FIRST, TABLE_CASE_WITH_NOTE
                ]
            ):

                balachesheet_result = parse_balancesheet_table(
                    table_dict['balance_sheet']['table'],
                    table_dict['balance_sheet']['unit'],
                    table_dict['balance_sheet']['style'],
                )
                self.result.update(balachesheet_result)
        except exception_utils.TableStyleError:
            message = (
                f"Unexpected balancesheet table {self.link} {self.table_link}"
            )
            logger.error(message)
            raise exception_utils.ExpectedError(message)
        if table_dict.get('summary') and table_dict['summary']['style'] in [
                TABLE_CASE_FIRST, TABLE_CASE_WITH_NOTE
        ]:
            root = LH.fromstring(table_dict['summary']['table'])
            unit = table_dict['summary']['unit']
            self.fill_insufficient_petovski_data(
                petovski_balancesheet_dict, root, unit
            )
            self.fill_insufficient_petovski_data(
                petovski_income_statement_dict, root, unit
            )
            self.fill_insufficient_petovski_data(
                petovski_cash_dict, root, unit
            )
        if not self.result.get('net_income'):
            result_key_list = deepcopy(list(self.result.keys()))
            for key in result_key_list:
                if re.search(
                    petovski_income_statement_dict['net_income'], 
                    key
                ):
                    self.result['net_income'] = self.result[key]

        if (
            not table_dict.get('cashflow') 
            and 
            table_dict['income_statement']['style'] == TABLE_CASE_FIRST 
        ):
            root = LH.fromstring(table_dict['income_statement']['table'])
            try:
                operational_income = self.result['operational_income']
                depreciation_cost = parse_row_data_with_re(
                    root,
                    petovski_sub_income_dict['depreciation_cost'],
                    table_dict['income_statement']['unit']
                )
                tax_costs = parse_row_data_with_re(
                    root,
                    petovski_sub_income_dict['corporate_tax_cost'],
                    table_dict['income_statement']['unit']
                )
                try:
                    current_assets = self.result.get('current_assets')
                    current_debt = self.result.get('current_debt')
                    current_working_capital = current_assets - current_debt
                    balance_elem = LH.fromstring(
                        table_dict['balance_sheet']['table']
                    )
                    last_year_ca = parse_last_year_row(
                        balance_elem,
                        petovski_balancesheet_dict['current_assets'],
                        table_dict['balance_sheet']['unit'],
                        data_key='current_assets'
                    )
                    last_year_cb = parse_last_year_row(
                        balance_elem,
                        petovski_balancesheet_dict['current_debt'],
                        table_dict['balance_sheet']['unit'],
                        data_key='current_debt'
                    )
                    self.result['last_year_current_assets'] = last_year_ca
                    self.result['last_year_current_debt'] = last_year_cb
                    last_year_working_capital = last_year_ca - last_year_cb
                    working_capital_change = (
                        current_working_capital - last_year_working_capital
                    )
                except Exception as e:
                    print('Error occur while calc working capital', e)
                    working_capital_change = 0
                self.result['cashflow_from_operation'] = (
                    operational_income + 
                    depreciation_cost - 
                    tax_costs + 
                    working_capital_change
                )
                self.result['direct_cashflow'] = False
            except Exception as e:
                message = (
                    f"Error occur while parsing no cashtable data {self.link} "
                    f"{self.table_link} " 
                    + str(e)
                )
                logger.error(message)
                raise exception_utils.ExpectedError(message)
        cash_remained_keys = return_remained_petovski_data(
            self.result, petovski_cash_dict
        )
        balancesheet_remained_keys = return_remained_petovski_data(
            self.result,
            petovski_balancesheet_dict
        )
        incomestatement_remained_keys = return_remained_petovski_data(
            self.result,
            petovski_income_statement_dict
        )
        if (
            cash_remained_keys or balancesheet_remained_keys 
            or incomestatement_remained_keys
        ):
            driver_case_list = [
                TABLE_CASE_SECOND,
                TABLE_CASE_THIRD
            ]
            
            if (
                table_dict['balance_sheet']['style'] in driver_case_list
                or
                table_dict['cashflow']['style'] in driver_case_list
                or 
                table_dict['income_statement']['style'] in driver_case_list
            ):
                driver_result = return_driver_report_data(
                    self.table_link, table_dict, self.result
                )
                self.result.update(driver_result)
        self.result = fillin_insufficient_balance_data(self.result)
        cash_remained_keys = return_remained_petovski_data(
            self.result, petovski_cash_dict
        )
        balancesheet_remained_keys = return_remained_petovski_data(
            self.result,
            petovski_balancesheet_dict
        )
        incomestatement_remained_keys = return_remained_petovski_data(
            self.result,
            petovski_income_statement_dict
        )

        if cash_remained_keys:
            em = (
                f"Not Sufficient cashflow with url {self.link} "
                f"{self.table_link} {','.join(cash_remained_keys)}"
            )
            logger.error(em)
            raise exception_utils.ExpectedError(em)
        if balancesheet_remained_keys:
            em = (
                f"Not Sufficient balancesheet with url {self.link} "
                f"{self.table_link} "
                f"{','.join(list(balancesheet_remained_keys))}"
            )
            logger.error(em)
            raise exception_utils.ExpectedError(em)
        if incomestatement_remained_keys:
            em = (
                f"Not Sufficient income statements with url "
                f"{self.table_link} {self.link} "
                f"{','.join(incomestatement_remained_keys)} "
            )
            logger.error(em)
            raise exception_utils.ExpectedError(em)
        return self.result
