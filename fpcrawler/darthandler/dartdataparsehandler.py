from typing import Tuple, List
import lxml.html as LH
import re
from bs4 import BeautifulSoup
import copy
from fp_types import (
    none_minus_keys,
    CONNECTED_BALANCE_RE,
    CONNECTED_CASHFLOW_RE,
    CONNECTED_INCOME_RE,
    BALANCE_RE,
    CASHFLOW_RE,
    INCOME_RE,
    TABLE_CASE_FIRST,
    TABLE_CASE_SECOND,
    TABLE_CASE_THIRD,
    TABLE_CASE_WITH_NOTE,
    petovski_balancesheet_dict,
    petovski_cash_dict,
    petovski_income_statement_dict,
    petovski_sub_income_dict,
    retypes,
    balance_data_re_list,
    income_data_re_list,
    cashflow_data_re_list,
    summary_re_list

)
from utils import exception_utils
from scrapy.http import Request, HtmlResponse
from scrapy.selector import Selector
import logging

logger = logging.getLogger()


def find_value(
    text: str, 
    unit: int,
    data_key: str = "", 
    is_minus_data=False, 
    **kwargs
):
    is_none_minus = True if data_key and data_key in none_minus_keys else False
    re_data = re.search('(\<td.*\>)', text)
    if re_data:
        td_tag = re_data.group(1)
        text = text.replace(td_tag, "")        
    final_text = (
        text.replace(" ", "")
        .replace("△", "-")
        .replace("(-)", "-")
        .replace(",", "")
        .replace("=", "")
        .replace('원', '')
        .replace('주', '')
        .replace('Δ', '-')
        .replace("_", "")
        .replace('(', '-')
        .replace(')', '')
    )
    final_text = final_text\
        .replace('<td>', '')\
        .replace('</td>', '')\
        .replace('&#13;', '')\
        .strip()
    if not final_text:
        raise ValueError
    try:
        float_value = float(final_text)
    except ValueError:
        if len(re.findall('\.', final_text)) >= 2:
            float_value = float(final_text.replace('.', ''))
        else:
            raise ValueError
    if is_minus_data and float_value > 0:
        return float_value * unit * -1
    # if data_key in ['net_income', 'operational_income'] and :
    if is_none_minus and float_value < 0:
        return float_value * unit * -1
    return float_value * unit


def parse_table_style_case(root: str):
    root = LH.fromstring(root)
    # 데이터에 주석이 있는지 확인하기
    note_re = '주[\s]*석'
    seconds_head_rows = root.xpath(f".//thead/tr/th[2]/text()")
    if seconds_head_rows:
        if re.match(note_re, seconds_head_rows[0]):
            return TABLE_CASE_WITH_NOTE
    all_rows = root.xpath('.//tbody//tr')
    if len(all_rows) > 3 and len(all_rows[0].getchildren()) >= 2:
        return TABLE_CASE_FIRST
    elif len(all_rows) == 1 and len(all_rows[0].getchildren()) >= 2:
        return TABLE_CASE_SECOND
    elif len(all_rows) == 2 and len(all_rows[0].getchildren()) >= 2:
        return TABLE_CASE_THIRD
    else:
        raise exception_utils.TableStyleError


def get_financial_statesments_links(
    soup2: BeautifulSoup, 
    head_lines: list, 
    line_find: int
) -> str:
    if len(head_lines) - 1 < line_find+4:
        return ""
    line_words = head_lines[line_find+4].split("'")
    rcpNo = line_words[1]
    dcmNo = line_words[3]
    eleId = line_words[5]
    offset = line_words[7]
    length = line_words[9]

    dart = soup2.find_all(string=re.compile('dart.dtd'))
    dart2 = soup2.find_all(string=re.compile('dart2.dtd'))
    dart3 = soup2.find_all(string=re.compile('dart3.xsd'))

    if len(dart3) != 0:
        link2 = (
            "http://dart.fss.or.kr/report/viewer.do?rcpNo=" 
            + rcpNo + "&dcmNo=" + dcmNo + "&eleId=" + eleId 
            + "&offset=" + offset + "&length=" + length 
            + "&dtd=dart3.xsd"
        )
    elif len(dart2) != 0:
        link2 = (
            "http://dart.fss.or.kr/report/viewer.do?rcpNo=" 
            + rcpNo + "&dcmNo=" + dcmNo + "&eleId=" + eleId 
            + "&offset=" + offset + "&length=" + length 
            + "&dtd=dart2.dtd"
        )
    elif len(dart) != 0:
        link2 = (
            "http://dart.fss.or.kr/report/viewer.do?rcpNo=" + rcpNo
            + "&dcmNo=" + dcmNo + "&eleId=" + eleId + "&offset="
            + offset + "&length=" + length + "&dtd=dart.dtd"
        )
    else:
        link2 = (
            "http://dart.fss.or.kr/report/viewer.do?rcpNo=" 
            + rcpNo + "&dcmNo=" + dcmNo + "&eleId=0&offset=0&length=0&dtd=HTML"
        )
    return link2


def parse_stockcount_section_link(soup: BeautifulSoup) -> str:
    head_lines = soup.find('head').text.split("\n")
    hangul = re.compile('[^ ㄱ-ㅣ가-힣]+')
    for idx, head_line in enumerate(head_lines):
        hangul = " ".join(re.split('[^ ㄱ-ㅣ가-힣]+', head_line))
        if '주식의 총수' in hangul:
            link_stock_count = get_financial_statesments_links(
                soup, 
                head_lines, 
                idx
            )
            return link_stock_count
    raise ValueError("No stockcount links available")


def parse_units(data: Selector):
    unit_ptag_text = data.extract()
    unit_regex = re.compile("(\d+(,\d+)*)원")
    if unit_regex.search(unit_ptag_text):
        unit = unit_regex.search(unit_ptag_text).group(1)
        unit = float(unit.replace(',', ''))
    else:
        unit_regex = re.compile("\s*\S*원")
        unit = unit_regex.search(unit_ptag_text).group()

        if '백만' in unit:
            unit = 1000000
        elif '십만' in unit:
            unit = 100000
        elif '천만' in unit or '1000 만' in unit:
            unit = 100000000
        elif '천' in unit:
            unit = 1000
        elif '만' in unit:
            unit = 10000
        elif '백' in unit:
            unit = 100
        else:
            unit = 1
    return unit 


def return_stockcount_data(link: str, soup: BeautifulSoup) -> dict:
    request = Request(url=link)
    scrapyResponse = HtmlResponse(
        url=link, 
        request=request, 
        body=str(soup), 
        encoding='utf-8'
    )        
    data_re = "발행[ \s]*주[ \s]*식.*총[ \s]*수"
    table_list = scrapyResponse.xpath('//table')
    if len(table_list) == 0:
        raise exception_utils.NotableError
    return_data = {}
    for table in table_list:
        total_count_row = table.xpath(
            f".//tr/td[1][re:match(text(),'{data_re}')]/.."
        )
        if total_count_row: 
            td_list = total_count_row.xpath('./td')
            if len(td_list) == 2:
                td_text = td_list[1].xpath('./text()').extract()
                data_value = find_value(td_text[0], 1)
                return {
                    'total_stock_count': data_value
                }
            else: 
                value_list = []
                for td in td_list[-1::-1]:
                    try:
                        td_text = td.xpath('./text()').extract()
                        td_text = td_text[0]
                        total_count = find_value(td_text, 1)
                        value_list.append(total_count)
                    except Exception:
                        pass
                if value_list:
                    return_data['total_stock_count'] = value_list[0]
                    if len(value_list) >= 3:
                        return_data['common_stock_count'] = value_list[2]
                        return_data['preferred_stock_count'] = value_list[1]
                    return return_data

    raise ValueError(f"No Stock data exists {link}")


def parse_finance_table(
    re_data: str, 
    scrapyResponse: HtmlResponse, 
    re_data_list: list = []
) -> Tuple[str, int]:
    check_unit_re = "단\s*위"        
    # 단위정보 및 테이블명이 하나의 테이블에있고 다음테이블에 관련데이터가 있는경우
    first_re_data = re_data_list[0]
    first_style_balance_table_list = scrapyResponse.xpath(
        f"//table//*[re:match(text(), '{re_data}')]"
        "/ancestor::table/following-sibling::table[1]"
        f"//*[re:match(text(), '{first_re_data}')]/ancestor::table"
    )
    if first_style_balance_table_list:
        data_table = first_style_balance_table_list[0]
        unit_table = data_table.xpath(
            "./preceding-sibling::table[1]"
            f"//*[re:match(text(),'{check_unit_re}')]"
        )
        if unit_table:
            unit = parse_units(unit_table[0])
            data_table = data_table.extract()
            return data_table, unit

    # 테이블재목의 ptag 다음으로 단위를 포함하고있는 ptag 다음으로 데이터를 포함하고 있는 table 이 같은 level위에 
    # 나타난 경우
    second_style_table_list = scrapyResponse.xpath(
        f"//p[re:match(text(), '{re_data}')]"
        "/following-sibling::p[1]/following-sibling"
        f"::table[1]//*[re:match(text(), '{first_re_data}')]"
        "/ancestor::table/preceding-sibling::p[1]"
    )
    if second_style_table_list:
        if re.search(check_unit_re, second_style_table_list[0].extract()):
            unit = parse_units(second_style_table_list[0])
            data_table = second_style_table_list[0].xpath(
                "./following-sibling::table[1]//"
                f"*[re:match(text(), '{first_re_data}')]/ancestor::table"
            )
            data_table = data_table[0].extract()
            return data_table, unit

    # 테이블 재목과 단위를 동시에 포함하는 ptag 데이터를 포함하고 있는 table 이 같은 level위에 나타나는경우.
    third_style_table_list = scrapyResponse.xpath(
        f"//p[re:match(text(), '{re_data}')]"
        "/following-sibling::table[1]"
        f"//*[re:match(text(), '{first_re_data}')]"
        "/ancestor::table"
    )
    if third_style_table_list:
        table = third_style_table_list[0]
        unit_p_tag = table.xpath('./preceding-sibling::p[1]')
        if unit_p_tag and re.search(check_unit_re, unit_p_tag[0].extract()):
            unit = parse_units(unit_p_tag[0])
            data_table = table.extract()
            return data_table, unit

    # 테이블 재목을 포함하는 ptag 단위를 표현하는 table 데이터를 포함하고 있는 table 이 같은 level위에 나타나는경우.
    fourth_style_table_list = scrapyResponse.xpath(
        f"//p[re:match(., '{re_data}')]"
        "/following-sibling::table[1]"
        "//*[re:match(.,'단[\s]*위')]"
        "/ancestor::table/following-sibling"
        f"::table[1]//*[re:match(text(), '{first_re_data}')]"
    )
    if fourth_style_table_list:
        table = fourth_style_table_list[0]
        unit_p_tag = table.xpath('./preceding-sibling::table[1]')
        if unit_p_tag and re.search(check_unit_re, unit_p_tag[0].extract()):
            unit = parse_units(unit_p_tag[0])
            data_table = table.extract()
            return data_table, unit

    first_style_balance_table_list = scrapyResponse.xpath(
        f"//table//p[re:match(text(), '{re_data}')]"
        "/ancestor::table/following-sibling::table"
        f"| //table//p[re:match(text(), '{re_data}')]/ancestor::table"
    )
    if len(first_style_balance_table_list) >= 2:
        unit = parse_units(first_style_balance_table_list[0])
        balancesheet_table = first_style_balance_table_list[1].extract()
        return balancesheet_table, unit
    else:
        first_style_balance_table_list = scrapyResponse.xpath(
            f"//p[re:match(text(), '{re_data}')]"
            "/following-sibling::table"
        )
        if len(first_style_balance_table_list) == 1:
            unit_p_tag = scrapyResponse.xpath(
                f"//p[re:match(text(), '{re_data}')]"
            )
            try:
                unit = parse_units(unit_p_tag[0])
                table_data = first_style_balance_table_list[0].extract()
                return table_data, unit
            except Exception: 
                pass
        if len(first_style_balance_table_list) >= 2:
            try:
                unit_table = first_style_balance_table_list[0]
                unit = parse_units(unit_table)
                balancesheet_table = (
                    first_style_balance_table_list[1].extract()
                )
                return balancesheet_table, unit
            except Exception:
                pass
        data_regex_and_cond_list = [
            (
                f".//td[re:match(text(),'{reg_data}')] "
                "or .//p[re:match(text(),'{reg_data}')]"
            )
            for reg_data in re_data_list
        ]
        reg_data = ' and '.join(data_regex_and_cond_list)  
        data_table_xpath = f'//table[{reg_data}]'
        data_table = scrapyResponse.xpath(data_table_xpath)
        unit_table = scrapyResponse.xpath(
            data_table_xpath + "/preceding-sibling::table[1]"
        )
        if (
            data_regex_and_cond_list and data_table and unit_table
        ):
            try:
                unit_table = unit_table[0]
                unit = parse_units(unit_table)
                balancesheet_table = data_table[0].extract()
                return balancesheet_table, unit
            except Exception:
                pass
        table_list = scrapyResponse.xpath('.//table')
        for table in table_list:
            td_text = ''.join(
                [x.extract() for x in table.xpath('.//td/text()')]
            )
            all_matches = all(
                [re.search(reg_re, td_text) for reg_re in re_data_list]
            )
            if all_matches:
                unit_table = table.xpath("./preceding-sibling::table[1]")
                unit_table = unit_table[0]
                unit = parse_units(unit_table)
                balancesheet_table = table.extract()
                return balancesheet_table, unit
        unit_table = scrapyResponse.xpath(
            f"//table//td[re:match(text(), '{re_data}')]/ancestor::table"
        )
        data_table = scrapyResponse.xpath(
            f"//table//td[re:match(text(), '{re_data}')]"
            "/ancestor::table/following-sibling::table"
        )
        if unit_table and data_table:
            unit = parse_units(unit_table[0])
            balancesheet_table = data_table[0].extract()
            return balancesheet_table, unit
    raise ValueError("No table")


def return_financial_report_table(
    link: str, 
    soup: BeautifulSoup, 
    is_connected_inside_normal: bool = False
) -> dict:
    return_data = {}
    request = Request(url=link)
    scrapyResponse = HtmlResponse(
        url=link, request=request, body=str(soup), encoding='utf-8'
    )        
    extra_re = "요[ \s]*약.*재[ \s]*무[ \s]*정[\s]*보"
    all_table_list = scrapyResponse.xpath("//table")
    if is_connected_inside_normal:
        balance_re = CONNECTED_BALANCE_RE
        cashflow_re = CONNECTED_CASHFLOW_RE
        income_re = CONNECTED_INCOME_RE
    else:
        balance_re = BALANCE_RE
        cashflow_re = CASHFLOW_RE
        income_re = INCOME_RE

    if len(all_table_list) < 2:
        raise exception_utils.NotableError

    try:
        balancesheet_table, unit = parse_finance_table(
            balance_re,
            scrapyResponse,
            balance_data_re_list
        )
        table_style = parse_table_style_case(balancesheet_table)
        return_data['balance_sheet'] = {}
        return_data['balance_sheet']['table'] = balancesheet_table
        return_data['balance_sheet']['unit'] = unit
        return_data['balance_sheet']['style'] = table_style
    except exception_utils.TableStyleError as te: 
        error_message = f"No balancesheet data {link} " + str(te) 
        logger.error(error_message)
        raise exception_utils.NotableError(error_message)
    except ValueError as ve:
        message = f"No balancesheet data {link} " + str(ve) 
        logger.error(message)
        raise exception_utils.NotableError(message)
    except Exception as e:
        error_message = f"No balancesheet data unexpected {link} " + str(e)
        logger.error(error_message)
        raise exception_utils.NotableError(error_message)

    try:
        income_table, unit = parse_finance_table(
            income_re,
            scrapyResponse,
            income_data_re_list
        )
        table_style = parse_table_style_case(income_table)
        return_data['income_statement'] = {}
        return_data['income_statement']['table'] = income_table
        return_data['income_statement']['unit'] = unit
        return_data['income_statement']['style'] = table_style
    except exception_utils.TableStyleError as te: 
        error_message = f"No income statement data {link} " + str(te) 
        logger.error(error_message)
        raise exception_utils.NotableError(error_message)
    except ValueError as ve:
        logger.error("No income statement data" + link + str(ve))
        raise exception_utils.NotableError(error_message)

    try:
        cashflow_table, unit = parse_finance_table(
            cashflow_re,
            scrapyResponse,
            cashflow_data_re_list
        )
        table_style = parse_table_style_case(cashflow_table)
        return_data['cashflow'] = {}
        return_data['cashflow']['table'] = cashflow_table
        return_data['cashflow']['unit'] = unit
        return_data['cashflow']['style'] = table_style
    except Exception:
        pass

    try:
        summary_table, unit = parse_finance_table(
            extra_re,
            scrapyResponse,
            summary_re_list
        )
        table_style = parse_table_style_case(summary_table)
        return_data['summary'] = {}
        return_data['summary']['table'] = summary_table
        return_data['summary']['unit'] = unit
        return_data['summary']['style'] = table_style
    except Exception:
        pass
    return return_data


def check_is_table_ascending(header_list: List[Selector]):
    header_num_list = []
    for header in header_list:
        header_text = header.text_content()
        if re.search('(\d+)기', header_text):
            number = re.search('(\d+)기', header_text).group(1)
            header_num_list.append(float(number))
        else:
            if re.search('(\d+)[년|분기]', header_text):
                number = re.search('(\d+)[년|분기]', header_text).group(1)
                header_num_list.append(float(number))
    if len(header_num_list) >= 2:
        return header_num_list[0] >= header_num_list[-1]
    return True


def parse_table_data(
    root: HtmlResponse, 
    unit: int, 
    data_dict: dict, 
    style: str = TABLE_CASE_FIRST
):
    final_data_dict = {}
    # header를 parsing해서 가장 최근 데이터만 가져옴
    all_rows = root.xpath('.//tbody//tr')
    
    if style == TABLE_CASE_FIRST or style == TABLE_CASE_WITH_NOTE:
        header_list = root.xpath('.//thead//th')
        header_list = header_list[1:]
        ascending = check_is_table_ascending(header_list)
        for key in data_dict:
            try:
                if not final_data_dict.get(key):
                    value = parse_row_data_with_re(
                        root,
                        data_dict[key],
                        unit,
                        ascending,
                        data_key=key, 
                        style=style
                    )
                    final_data_dict[key] = value
            except ValueError:
                continue
        all_rows = root.xpath('//tr')
        for row in all_rows:
            td_list = row.xpath('.//td')
            if len(td_list) >= 2:
                td_key = td_list[0].text_content().strip().replace('.', '')
                number_string = (
                    td_list[1].text_content().strip() 
                    if ascending else 
                    td_list[-1].text_content().strip()
                )
                try:
                    if not final_data_dict.get(td_key):
                        final_data_dict[td_key] = find_value(
                            number_string,
                            unit,
                            td_key=td_key
                        )
                except Exception:
                    pass
    return final_data_dict


def parse_row_data_with_re(
    root: HtmlResponse,
    key: str,
    unit: int,
    is_ascending: bool = True,
    data_key: str = "", 
    style=None
):
    is_minus_data = False
    if data_key in ['net_income', 'operational_income']:
        xpath_expression = f"//tr//*[re:match(text(),'{key}')]/text()"
        xpath_result = root.xpath(
            xpath_expression, 
            namespaces={'re': 'http://exslt.org/regular-expressions'}
        )
        if xpath_result:
            row_text = xpath_result[0]
            if (
                re.search('손[\s]*실', row_text) 
                and not re.search('이[\s]*익', row_text)
            ):
                is_minus_data = True
    odd_style_result = root.xpath(
        (
            f"//thead/tr[1]/td[4]/../../tr[1]/td[3]/../../..//"
            "*[re:match(text(),'{key}')]/ancestor::tr"
            f"|//thead/tr[1]/th[4]/../../..//tbody"
            "/tr[1]/td[7]/../..//*[re:match(text(),'{key}')]"
            "/ancestor::tr"
        ), 
        namespaces={'re': 'http://exslt.org/regular-expressions'}
    )
    if odd_style_result:
        td_results = odd_style_result[0].xpath('.//td')
        if len(td_results) == 7:
            if is_ascending:
                start_number = 1
                end_number = 3
            else:
                start_number = 5
                end_number = 7

            for idx, td in enumerate(td_results[start_number:end_number]):
                number_string = td.text_content().encode().decode()
                try:
                    value = find_value(
                        number_string,
                        unit,
                        data_key=data_key, 
                        is_minus_data=is_minus_data
                    )
                    return value 
                except Exception:
                    pass

    result = root.xpath(
        f"//tr//*[re:match(text(),'{key}')]/ancestor::tr", 
        namespaces={'re': 'http://exslt.org/regular-expressions'}
    )
    if result:
        td_results = result[0].xpath('.//td')
        values_td = td_results[1:] if is_ascending else td_results[:0:-1]
        for idx, td in enumerate(values_td):
            if idx == 0 and style == TABLE_CASE_WITH_NOTE:
                continue
            number_string = td.text_content().encode().decode()
            try:
                value = find_value(
                    number_string,
                    unit,
                    data_key=data_key, 
                    is_minus_data=is_minus_data
                )
                return value 
            except Exception:
                pass
    raise ValueError(f"No data cannot find {key}")


def parse_balancesheet_table(table: str, unit: int, table_style):
    root = LH.fromstring(table)
    result = parse_table_data(
        root,
        unit,
        petovski_balancesheet_dict,
        style=table_style
    )
    if not result.get("current_assets"):
        current_assets = 0
        current_debt = 0 
        total_debt = result.get("total_debt") 
        for key in result:
            if re.search(retypes.total_debt_re_key, key):
                total_debt = result[key]
            for cd_key in retypes.current_debt_res:
                if re.search(retypes.current_debt_res[cd_key], key):
                    current_debt += result[key]
                    break 

            for ca_key in retypes.current_assets_res:
                if re.search(retypes.current_assets_res[ca_key], key):
                    current_assets += result[key]
                    break 
        result['current_assets'] = current_assets
        result['current_debt'] = (
            current_debt if not result.get('current_debt') else 
            result.get('current_debt')
        )
        result['longterm_debt'] = total_debt - result['current_debt']
    return result 


def parse_cashflow_table(table: str, unit: int, table_style):
    root = LH.fromstring(table)
    result = parse_table_data(
        root, unit, petovski_cash_dict, style=table_style
    )
    return result


def parse_incomestatement_table(table, unit, table_style):
    root = LH.fromstring(table)
    result = parse_table_data(
        root,
        unit,
        petovski_income_statement_dict,
        style=table_style
    )
    if result.get("sales") is not None and result['sales'] == 0:
        result.pop('sales')
    # 은행업의 경우 매출이 나타나잊지 않으므로 operational_income을 sales대용으로활용
    if not result.get('sales'):
        result['sales'] = result['operational_income']
    if 'extra_ordinary_profit' not in result:
        result['extra_ordinary_profit'] = 0 
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

    if 'extra_ordinary_loss' not in result:
        result['extra_ordinary_loss'] = 0
    if 'operational_income' not in result:
        try:
            operational_sales = parse_row_data_with_re(
                root,
                petovski_sub_income_dict['operational_sales'],
                unit,
                data_key='operational_sales', style=table_style
            )
            operational_costs = parse_row_data_with_re(
                root,
                petovski_sub_income_dict['operational_costs'],
                unit,
                data_key='operational_sales',
                style=table_style
            )
            result['operational_income'] = (
                operational_sales - operational_costs
            )
        except ValueError:
            pass
    if 'gross_profit' not in result:
        result['gross_profit'] = result['operational_income']
    return result


def return_remained_petovski_data(result: dict, data_dict: dict) -> list:
    re_key_list = set(data_dict.keys())
    inserted_data_keys = set(result.keys())
    remained_keys = re_key_list - inserted_data_keys
    return list(remained_keys)


def fillin_insufficient_balance_data(result):
    if result.get("book_value") is None:
        if result.get("total_assets") and result.get("total_debt"):
            result['book_value'] = (
                result['total_assets'] - result['total_debt']
            )
    if result.get('current_debt') is None:
        if result.get("total_debt") and result.get("longterm_debt"):
            result['current_debt'] = (
                result['total_debt'] - result['longterm_debt']
            )
    return result
        

def parse_last_year_row(
    root: HtmlResponse,
    key: str,
    unit: int,
    is_ascending: bool = True,
    data_key: str = "", 
    style=None
):
    is_minus_data = False
    if data_key in ['net_income', 'operational_income']:
        xpath_expression = f"//tr//*[re:match(text(),'{key}')]/text()"
        xpath_result = root.xpath(
            xpath_expression, 
            namespaces={'re': 'http://exslt.org/regular-expressions'}
        )
        row_text = xpath_result[0]
        if re.search('손[\s]*실', row_text) and not re.search(
            '이[\s]*익', row_text
        ):
            is_minus_data = True
    odd_style_result = root.xpath(
        f".//thead/tr[1]/th[4]/../../"
        f"tr[1]/th[3]/../../..//*[re:match(text(),'{key}')]/ancestor::tr", 
        namespaces={'re': 'http://exslt.org/regular-expressions'}
    )
    if odd_style_result:
        td_results = odd_style_result[0].xpath('.//td')
        if len(td_results) == 7:
            for idx, td in enumerate(td_results[3:5]):
                number_string = td.text_content().encode().decode()
                try:
                    value = find_value(
                        number_string,
                        unit,
                        data_key=data_key, 
                        is_minus_data=is_minus_data
                    )
                    return value 
                except Exception:
                    pass
    result = root.xpath(
        f"//tr//*[re:match(text(),'{key}')]/ancestor::tr", 
        namespaces={'re': 'http://exslt.org/regular-expressions'}
    )
    if result:
        td_results = result[0].xpath('.//td')
        values_td = td_results[2:] if is_ascending else td_results[-2:0:-1]
        for idx, td in enumerate(values_td):
            if idx == 0 and style == TABLE_CASE_WITH_NOTE:
                continue
            number_string = td.text_content().encode().decode()
            try:
                value = find_value(
                    number_string,
                    unit,
                    data_key=data_key, 
                    is_minus_data=is_minus_data
                )
                return value 
            except Exception:
                pass
    raise ValueError(f"No data cannot find {key}")


def parse_unit_string(data):
    unit = 1
    unit_find = 0
    re_unit1 = re.compile('단위[ \s]*:[ \s]*원')
    re_unit2 = re.compile('단위[ \s]*:[ \s]*백만원')
    re_unit3 = re.compile('단위[ \s]*:[ \s]*천원')
    # 원
    if re_unit1.search(data):
        unit = 1
        unit_find = 1
    # 백만원
    elif re_unit2.search(data):
        unit = 1000000
        unit_find = 1
    elif re_unit3.match(data):
        unit = 1000
        unit_find = 1
    return unit