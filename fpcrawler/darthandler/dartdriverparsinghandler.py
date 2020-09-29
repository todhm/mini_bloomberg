from typing import List
import re
from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from utils.crawl_utils import return_driver
from utils import exception_utils
from fp_types import (
    petovski_cash_dict,
    petovski_balancesheet_dict,
    cashflow_data_re_list,
    petovski_income_statement_dict,
    income_data_re_list,
    balance_data_re_list,
    additional_balance_data,
    petovski_sub_income_dict
)
from .dartdataparsehandler import (
    return_remained_petovski_data,
    find_value
) 


def parsing_driver_table_data(
    driver: webdriver, 
    table_elem: WebElement, 
    reg_data: dict, 
    unit: int, 
    insert_span_needed: bool = True,
    additional_table_data: dict = {}
):
    result = {}
    td_list = table_elem.find_elements_by_xpath('.//td')
    for td in td_list:
        if not td.find_elements_by_xpath('.//span'):
            inner_html_string = '<br>'.join(
                [
                    '<span>'+x.strip()+'</span>' 
                    for x in td.get_attribute("innerHTML").split('<br>')
                ]
            )
            driver.execute_script(
                f"arguments[0].innerHTML='{inner_html_string}'", td
            )
    td_list = table_elem.find_elements_by_xpath('.//td')
    first_td = td_list[0]
    value_td_list = td_list[1:]
    value_span_list = [ 
        td.find_elements_by_xpath('.//span') 
        for td in value_td_list
    ]
    for span in first_td.find_elements_by_xpath('.//span'):
        span_text = span.text.strip()
        if span_text:
            is_re_caught = False       
            found_re = ''
            for table_re in reg_data:
                if result.get(table_re, None) is None and re.search(
                    reg_data[table_re], span_text
                ):
                    location = span.location
                    location_y = location.get('y')
                    is_re_caught = True
                    found_re = table_re
                    break 
            if not is_re_caught:
                for additional_re in additional_table_data:
                    if result.get(additional_re, None) is None and re.search(
                        additional_table_data[additional_re],
                        span_text
                    ):
                        location = span.location 
                        location_y = location.get('y')
                        is_re_caught = True
                        found_re = additional_re
                        break

            if is_re_caught:
                value = None
                for span_list in value_span_list:
                    value_found = False
                    for value_span in span_list:
                        # if table_re == 'total_assets':
                        #     print(span_text,'d'*100,location_y,span.location.get("y"),span.text)
                        if value_span.location.get("y") == location_y:
                            try:
                                value = find_value(
                                    value_span.text,
                                    unit,
                                    data_key=found_re
                                )
                                result[found_re] = value
                                value_found = True
                                break
                            except Exception: 
                                pass
                    if value_found:
                        break
    return result


def return_driver_matching_table(
    matching_table_list: List[WebElement], 
    re_list: List[str]
):
    for idx, table in enumerate(matching_table_list):
        reg_data = ' and '.join(re_list)  
        if re.search(reg_data, table.text):
            try:
                return table
            except Exception: 
                continue
    raise ValueError("No matching driver table")


def return_driver_report_data(link: str, table_dict: dict, result: dict = {}):
    try:
        driver = return_driver()
        driver.get(link)
    except Exception as e: 
        em = f"Error connect to link with driver {link}  {str(e)}"
        raise ValueError(em)

    matching_table_list = driver.find_elements_by_xpath(
        "//p/following-sibling::table"
        "/tbody[count(descendant::tr)=1 or count(descendant::tr)=2]/.. "
    )
    if table_dict.get('cashflow'):
        try:
            cash_remained_keys = return_remained_petovski_data(
                result, petovski_cash_dict
            )
            if cash_remained_keys:
                cashflow_table = return_driver_matching_table(
                    matching_table_list,
                    cashflow_data_re_list
                )
                cashflow_result = parsing_driver_table_data(
                    driver,
                    cashflow_table,
                    petovski_cash_dict,
                    table_dict['cashflow']['unit']
                )
            else:
                cashflow_result = {}
        except Exception as e:
            em = f"Error whil making driver cashflow table {link} {str(e)}"
            raise ValueError(em)

    else:
        cashflow_result = {}
    try:
        remained_income_keys = return_remained_petovski_data(
            result, petovski_income_statement_dict
        )
        if remained_income_keys:
            income_table = return_driver_matching_table(
                matching_table_list,
                income_data_re_list
            )
            incomestatement_result = parsing_driver_table_data(
                driver,
                income_table,
                petovski_income_statement_dict,
                table_dict['income_statement']['unit']
            )
        else:
            incomestatement_result = {}
    except Exception as e:
        em = f"Error while making driver income table {link} {str(e)}" 
        raise ValueError(em)

    try:
        remained_balance_keys = return_remained_petovski_data(
            result, 
            petovski_balancesheet_dict
        )
        if remained_balance_keys:
            balance_table = return_driver_matching_table(
                matching_table_list,
                balance_data_re_list
            )
            balachesheet_result = parsing_driver_table_data(
                driver,
                balance_table,
                petovski_balancesheet_dict,
                table_dict['balance_sheet']['unit'],
                additional_table_data=additional_balance_data
            )
        else:
            balachesheet_result = {}
    except Exception as e:
        em = f"Error while making driver balance {link} {str(e)}" 
        raise ValueError(em)

    if incomestatement_result and not incomestatement_result.get(
        'extra_ordinary_loss'
    ):
        incomestatement_result['extra_ordinary_loss'] = 0 
    if incomestatement_result and not incomestatement_result.get(
        'extra_ordinary_profit'
    ):
        incomestatement_result['extra_ordinary_profit'] = 0 
    if not incomestatement_result.get('net_income') and cashflow_result.keys():

        cashflow_table = return_driver_matching_table(
            matching_table_list,
            cashflow_data_re_list
        )
        netincome_result = parsing_driver_table_data(
            driver,
            cashflow_table,
            {'net_income': petovski_income_statement_dict['net_income']},
            table_dict['balance_sheet']['unit'],
            insert_span_needed=False
        )
        incomestatement_result['net_income'] = netincome_result.get(
            'net_income'
        )
    if not table_dict.get('cashflow'):
        try:
            income_sub_result = parsing_driver_table_data(
                driver,
                income_table,
                {
                    'depreciation_cost': petovski_sub_income_dict[
                        'depreciation_cost'
                    ],
                    'corporate_tax_cost': petovski_sub_income_dict[
                        'corporate_tax_cost'
                    ],
                },
                table_dict['balance_sheet']['unit'],
                insert_span_needed=False
            )
            operational_income = incomestatement_result['operational_income']
            depreciation_cost = income_sub_result['depreciation_cost']
            corporate_tax_cost = income_sub_result['corporate_tax_cost']
            cashflow_result['cashflow_from_operation'] = (
                operational_income + depreciation_cost - corporate_tax_cost
            )
        except Exception as e:
            raise exception_utils.ExpectedError(
                f"Error occur while parsing no cashtable data {link}" + str(e)
            )
    result.update(cashflow_result)
    result.update(balachesheet_result)
    result.update(incomestatement_result)
    return result