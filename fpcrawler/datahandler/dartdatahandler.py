import copy
from datetime import datetime as dt, timedelta
import logging
import os
import random
import re
import string
import lxml.html as LH
import requests
import asyncio
import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from bs4 import BeautifulSoup
from scrapy.http import Request, HtmlResponse
from utils import exception_utils, headers
from utils.class_utils import DataHandlerClass
from utils.crawl_utils import return_driver
import xlrd
from fp_types import petovski_cash_dict, petovski_balancesheet_dict,\
    petovski_income_statement_dict, petovski_sub_income_dict, \
    petovski_re_dict, none_minus_keys, retypes
from fp_types import YEARLY_REPORT, QUARTER_REPORT, \
    CONNECTED_FINANCIAL_STATEMENTS, NORMAL_FINANCIAL_STATEMENTS
import fp_types


logger = logging.getLogger()


def find_value(text, unit, data_key="", is_minus_data=False, **kwargs):
    is_none_minus = True if data_key and data_key in none_minus_keys else False
    re_data = re.search('(\<td.*\>)', text)
    if re_data:
        td_tag = re_data.group(1)
        text = text.replace(td_tag, "")        
    final_text = text.replace(" ", "")\
        .replace("△", "-")\
        .replace("(-)", "-")\
        .replace(",", "")\
        .replace("=", "")\
        .replace('원', '')\
        .replace('주', '')\
        .replace('Δ', '-')\
        .replace("_", "")\
        .replace('(', '-')\
        .replace(')', '')
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
    

TABLE_CASE_FIRST = "TABLE_CASE_FIRST_MULTIPLE_TRS"
TABLE_CASE_WITH_NOTE = "TABLE_CASE_WITH_NOTE"
TABLE_CASE_SECOND = "TABLE_CASE_SECONDS_ONE_TRS"
TABLE_CASE_THIRD = "TABLE_CASE_THIRD_TWO_TRS"

balance_data_re_list = [
    f"{petovski_balancesheet_dict['current_assets']}"
    f"|{petovski_balancesheet_dict['total_assets']}",
    petovski_balancesheet_dict['current_debt'],
]
income_data_re_list = [
    petovski_income_statement_dict['operational_income'],
    petovski_income_statement_dict['gross_profit']
]
cashflow_data_re_list = [
    petovski_cash_dict['cashflow_from_operation']
]
summary_re_list = [
    petovski_balancesheet_dict['current_assets'],
    petovski_balancesheet_dict['current_debt'],
    petovski_income_statement_dict['operational_income'],
    petovski_income_statement_dict['net_income']
]


def parse_stockcount_section_link(soup):
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

 
def get_financial_statesments_links(soup2, head_lines, line_find):
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
        link2 = "http://dart.fss.or.kr/report/viewer.do?rcpNo=" + rcpNo + "&dcmNo=" + dcmNo + "&eleId=" + eleId + "&offset=" + offset + "&length=" + length + "&dtd=dart3.xsd"
    elif len(dart2) != 0:
        link2 = "http://dart.fss.or.kr/report/viewer.do?rcpNo=" + rcpNo + "&dcmNo=" + dcmNo + "&eleId=" + eleId + "&offset=" + offset + "&length=" + length + "&dtd=dart2.dtd"
    elif len(dart) != 0:
        link2 = "http://dart.fss.or.kr/report/viewer.do?rcpNo=" + rcpNo + "&dcmNo=" + dcmNo + "&eleId=" + eleId + "&offset=" + offset + "&length=" + length + "&dtd=dart.dtd"
    else:
        link2 = "http://dart.fss.or.kr/report/viewer.do?rcpNo=" + rcpNo + "&dcmNo=" + dcmNo + "&eleId=0&offset=0&length=0&dtd=HTML"  
    return link2 


def return_stockcount_data(link, soup):
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

    # for table in all_table_list:
    #     table.find("")

class DartDataHandler(DataHandlerClass):


    def __init__(self, *args, **kwargs):
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14'
        self.headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        


    async def return_async_get_soup(self, url, proxy_object=None):
        try:
            if proxy_object:
                proxy_ip = proxy_object['ip']
                proxy_port = proxy_object['port']
                http_proxy = f"http://{proxy_ip}:{proxy_port}"
                async with self.session.get(
                    url, headers=headers, proxy=http_proxy
                ) as resp:
                    result_string = await resp.text()
                    soup = BeautifulSoup(result_string)
                    return soup
            else:
                async with self.session.get(url, headers=headers) as resp:
                    result_string = await resp.text()
                    soup = BeautifulSoup(result_string, 'html.parser')
                    return soup
        except Exception as e:
            raise ValueError(f"Error making async requests {url}" + str(e))


    def parse_single_page_data(
        self, code, currentPage, 
        report_type, company_name, 
        start_date=None, textCrpCik=None
    ):
        url = 'http://dart.fss.or.kr/corp/searchExistAll.ax'
        today_string = dt.strftime(dt.now(),'%Y%m%d')
        start_date = start_date if start_date else '19980101'
        post_data = {
            'currentPage': currentPage,
            'maxResults': 1000,
            'maxLinks': 1000,
            'sort': 'date',
            'series': 'desc',
            'reportNamePopYn': 'N',
            'textCrpNm': code,
            'textPresenterNm': '',
            'startDate': start_date,
            'endDate': today_string,
            'finalReport': 'recent',
            'typesOfBusiness': 'all',
            'corporationType': 'all',
            'closingAccountsMonth': 'all',
            'reportName': report_type,
        }
        tpdata = {'textCrpNm': company_name}
        if textCrpCik:
            post_data['textCrpCik'] = textCrpCik
        else:
            data = requests.post(url, data=tpdata,headers=self.headers)
            if data.status_code == 200:
                if data.text != 'null':
                    post_data['textCrpCik']  = data.text.strip()
                else:
                    tpdata = {'textCrpNm': code}
                    data = requests.post(url,data=tpdata)
                    if data.text != 'null' and data.status_code == 200:
                        post_data['textCrpCik']= data.text.strip()
        url = 'http://dart.fss.or.kr/dsab002/search.ax'
        result = requests.post(url,data=post_data,headers=self.headers)
        if result.status_code > 300 :
            return []
        soup = BeautifulSoup(result.text, 'html.parser', from_encoding='utf-8')
        table = soup.find('table')
        if not table:
            return []
        trs = table.findAll('tr')
        tr_list = trs[1:]
        data_object_list = []
        for tr in tr_list:
            tds = tr.findAll('td')
            if len(tds)<5:
                continue
            link = 'http://dart.fss.or.kr' + tds[2].a['href']
            date = tds[4].text.strip().replace('.', '-')
            corp_name = tds[1].text.strip()
            market = tds[1].img['title']
            title = " ".join(tds[2].text.split())
            reporter = tds[3].text.strip()
            data_object = {}
            data_object['code'] = str(code)
            data_object['link'] = link
            data_object['reg_date'] = date
            data_object['corp_name'] = corp_name
            data_object['market_type'] = market
            data_object['title'] = title
            data_object['period_type'] = report_type
            data_object['reporter'] = reporter
            data_object_list.append(data_object)
        return data_object_list

    def parse_dart_search_result(self,soup,code,report_type):
        table = soup.find('table')
        if not table:
            return []
        trs = table.findAll('tr')
        tr_list = trs[1:]
        data_object_list = []
        for tr in tr_list:
            tds = tr.findAll('td')
            if len(tds)<5:
                continue
            link = 'http://dart.fss.or.kr' + tds[2].a['href']
            date = tds[4].text.strip().replace('.', '-')
            corp_name = tds[1].text.strip()
            market = tds[1].img['title']
            title = " ".join(tds[2].text.split())
            reporter = tds[3].text.strip()
            data_object = {}
            data_object['code'] = str(code)
            data_object['link'] = link
            data_object['reg_date'] = date
            data_object['corp_name'] = corp_name
            data_object['market_type'] = market
            data_object['title'] = title
            data_object['period_type'] = report_type
            data_object['reporter'] = reporter
            data_object_list.append(data_object)
        return data_object_list

    def return_report_list_links(
            self, code, currentPage, report_type,
            company_name, start_date=None, textCrpCik=None,
            return_final=True
        ):
        url = 'http://dart.fss.or.kr/corp/searchExistAll.ax'
        today_string = dt.strftime(dt.now(),'%Y%m%d')
        start_date = start_date if  start_date else '19990101'
        post_data = {
            'currentPage': currentPage,
            'maxResults': 1000,
            'maxLinks': 1000,
            'sort': 'date',
            'series': 'desc',
            'reportNamePopYn': 'N',
            'textCrpNm': code,
            'textPresenterNm': '',
            'startDate': start_date,
            'endDate': today_string,
            'typesOfBusiness': 'all',
            'corporationType': 'all',
            'closingAccountsMonth': 'all',
            'reportName': report_type,
        }
        if return_final:
            post_data['finalReport'] = 'recent'

        tpdata = {'textCrpNm': company_name}
        if textCrpCik:
            post_data['textCrpCik'] = textCrpCik
        else:
            data = requests.post(url, data=tpdata)
            if data.status_code == 200:
                if data.text != 'null':
                    post_data['textCrpCik'] = data.text.strip()
                else:
                    tpdata = {'textCrpNm': code}
                    data = requests.post(url, data=tpdata)
                    if data.text != 'null' and data.status_code == 200:
                        post_data['textCrpCik'] = data.text.strip()

        url = 'http://dart.fss.or.kr/dsab002/search.ax'
        result = requests.post(url, data=post_data, headers=self.headers)
        if result.status_code > 300:
            return []

        soup = BeautifulSoup(result.text, 'html.parser', from_encoding='utf-8')
        return self.parse_dart_search_result(soup, code, report_type)

        
    def return_company_eq_offer_lists(self, code, company_name, reg_date=None):
        report_type = '유상증자'
        final_result = []
        start_date = dt.strftime(reg_date,'%Y%m%d') if reg_date else None
        currentPage = 1
        while True: 
            try:
                result = self.return_report_list_links(code,currentPage,report_type,company_name,start_date=start_date,textCrpCik=None,return_final=False)
                result = [  i for i in result ]
                if not result:
                    break 
                final_result.extend(result)
            except Exception as e:
                break 

            currentPage += 1
        return final_result


    def return_company_report_list(self,code,company_name,reg_date=None):
        report_type_list = [YEARLY_REPORT,QUARTER_REPORT]
        final_result = []
        for report_type in report_type_list:
            start_date = dt.strftime(reg_date,'%Y%m%d') if reg_date else None
            currentPage = 1
            while True: 
                try:
                    result = self.return_report_list_links(code,currentPage,report_type,company_name,start_date=start_date,textCrpCik=None)
                    if not result:
                        break 
                    final_result.extend(result)
                except Exception as e:
                    break 

                currentPage += 1
        return final_result

    def parse_unit_string(self,data):
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

    def parse_units(self,data):
        unit_ptag_text = data.extract()
        unit_regex = re.compile("(\d+(,\d+)*)원")
        if unit_regex.search(unit_ptag_text):
            unit = unit_regex.search(unit_ptag_text).group(1)
            unit = float(unit.replace(',',''))
        else:
            unit_regex = re.compile("\s*\S*원")
            unit = unit_regex.search(unit_ptag_text).group()

            if '백만' in unit:
                unit = 1000000
            elif '십만' in unit:
                unit = 100000
            elif '천만'  in unit or  '1000 만' in unit:
                unit = 100000000
            elif '천' in unit:
                unit = 1000
            elif '만' in unit:
                unit = 10000
            elif '백' in unit:
                unit = 100
            else :
                unit =1 
        return unit 




    def check_is_ascending(self,header_list):
        header_num_list = []
        for header in header_list:
            header_text = header.text_content()
            if re.search('(\d+)기',header_text):
                number = re.search('(\d+)기',header_text).group(1)
                header_num_list.append(float(number))
            else:
                if re.search('(\d+)[년|분기]',header_text):
                    number = re.search('(\d+)[년|분기]',header_text).group(1)
                    header_num_list.append(float(number))

        if len(header_num_list)>=2:
            return header_num_list[0] >= header_num_list[-1]
            


        return True
          

    def parse_finance_table(self,re_data,scrapyResponse,re_data_list=[]):
        check_unit_re = "단\s*위"        
        # 단위정보 및 테이블명이 하나의 테이블에있고 다음테이블에 관련데이터가 있는경우
        first_re_data = re_data_list[0]
        first_style_balance_table_list = scrapyResponse.xpath(f"//table//*[re:match(text(), '{re_data}')]/ancestor::table/following-sibling::table[1]//*[re:match(text(), '{first_re_data}')]/ancestor::table")
        if first_style_balance_table_list:
            data_table = first_style_balance_table_list[0]
            unit_table = data_table.xpath(f"./preceding-sibling::table[1]//*[re:match(text(),'{check_unit_re}')]")
            if unit_table:
                unit = self.parse_units(unit_table[0])
                data_table = data_table.extract()
                return data_table, unit

        # 테이블재목의 ptag 다음으로 단위를 포함하고있는 ptag 다음으로 데이터를 포함하고 있는 table 이 같은 level위에 나타나는경우.
        second_style_table_list = scrapyResponse.xpath(f"//p[re:match(text(), '{re_data}')]/following-sibling::p[1]/following-sibling::table[1]//*[re:match(text(), '{first_re_data}')]/ancestor::table/preceding-sibling::p[1]")
        if second_style_table_list:
            if re.search(check_unit_re,second_style_table_list[0].extract()):
                unit = self.parse_units(second_style_table_list[0])
                data_table = second_style_table_list[0].xpath(f"./following-sibling::table[1]//*[re:match(text(), '{first_re_data}')]/ancestor::table")
                data_table = data_table[0].extract()
                return data_table, unit

        # 테이블 재목과 단위를 동시에 포함하는 ptag 데이터를 포함하고 있는 table 이 같은 level위에 나타나는경우.
        third_style_table_list = scrapyResponse.xpath(f"//p[re:match(text(), '{re_data}')]/following-sibling::table[1]//*[re:match(text(), '{first_re_data}')]/ancestor::table")
        if third_style_table_list:
            table = third_style_table_list[0]
            unit_p_tag = table.xpath('./preceding-sibling::p[1]')
            if unit_p_tag and re.search(check_unit_re, unit_p_tag[0].extract()):
                unit = self.parse_units(unit_p_tag[0])
                data_table = table.extract()
                return data_table, unit

        # 테이블 재목을 포함하는 ptag 단위를 표현하는 table 데이터를 포함하고 있는 table 이 같은 level위에 나타나는경우.
        fourth_style_table_list = scrapyResponse.xpath(
            f"//p[re:match(., '{re_data}')]/following-sibling::table[1]//*[re:match(.,'단[\s]*위')]/ancestor::table/following-sibling::table[1]//*[re:match(text(), '{first_re_data}')]"
        )
        if fourth_style_table_list:
            table = fourth_style_table_list[0]
            unit_p_tag = table.xpath('./preceding-sibling::table[1]')
            if unit_p_tag and re.search(check_unit_re, unit_p_tag[0].extract()):
                unit = self.parse_units(unit_p_tag[0])
                data_table = table.extract()
                return data_table, unit

        first_style_balance_table_list = scrapyResponse.xpath(f"//table//p[re:match(text(), '{re_data}')]/ancestor::table/following-sibling::table | //table//p[re:match(text(), '{re_data}')]/ancestor::table")
        if len(first_style_balance_table_list)>=2:
            unit = self.parse_units(first_style_balance_table_list[0])
            balancesheet_table = first_style_balance_table_list[1].extract()
            return balancesheet_table,unit
        else:
            first_style_balance_table_list = scrapyResponse.xpath(f"//p[re:match(text(), '{re_data}')]/following-sibling::table")
            if len(first_style_balance_table_list) == 1:
                unit_p_tag = scrapyResponse.xpath(f"//p[re:match(text(), '{re_data}')]")
                try:
                    unit = self.parse_units(unit_p_tag[0])
                    table_data = first_style_balance_table_list[0].extract()
                    return table_data, unit
                except Exception: 
                    pass
            if len(first_style_balance_table_list) >=2:
                try:
                    unit_table = first_style_balance_table_list[0]
                    unit = self.parse_units(unit_table)
                    balancesheet_table = first_style_balance_table_list[1].extract()
                    return balancesheet_table,unit
                except Exception as e:
                    pass
            data_regex_and_cond_list = [ f".//td[re:match(text(),'{reg_data}')] or .//p[re:match(text(),'{reg_data}')]" for reg_data in re_data_list]
            reg_data = ' and '.join(data_regex_and_cond_list)  
            data_table_xpath = f'//table[{reg_data}]'
            data_table = scrapyResponse.xpath(data_table_xpath)
            unit_table = scrapyResponse.xpath(data_table_xpath + "/preceding-sibling::table[1]")
            if data_regex_and_cond_list  and data_table  and unit_table:
                try:
                    unit_table = unit_table[0]
                    unit = self.parse_units(unit_table)
                    balancesheet_table = data_table[0].extract()
                    return balancesheet_table,unit
                except:
                    pass
            table_list = scrapyResponse.xpath('.//table')
            for table in table_list:
                td_text =''.join([x.extract() for x in table.xpath('.//td/text()')])
                all_matches = all([ re.search(reg_re,td_text) for reg_re in re_data_list])
                if all_matches:
                    unit_table = table.xpath("./preceding-sibling::table[1]")
                    unit_table = unit_table[0]
                    unit = self.parse_units(unit_table)
                    balancesheet_table = table.extract()
                    return balancesheet_table,unit
            unit_table = scrapyResponse.xpath(f"//table//td[re:match(text(), '{re_data}')]/ancestor::table")
            data_table = scrapyResponse.xpath(f"//table//td[re:match(text(), '{re_data}')]/ancestor::table/following-sibling::table")
            if unit_table and data_table:
                unit = self.parse_units(unit_table[0])
                balancesheet_table = data_table[0].extract()
                return balancesheet_table,unit
        raise ValueError("No table")


    def return_financial_report_table(self,link,soup, is_connected_inside_normal=False):
        return_data = {}
        request = Request(url=link)
        scrapyResponse = HtmlResponse(url=link, request=request, body=str(soup),encoding='utf-8')        
        extra_re = "요[ \s]*약.*재[ \s]*무[ \s]*정[\s]*보"
        all_table_list = scrapyResponse.xpath("//table")
        if is_connected_inside_normal:
            balance_re = fp_types.CONNECTED_BALANCE_RE
            cashflow_re = fp_types.CONNECTED_CASHFLOW_RE
            income_re = fp_types.CONNECTED_INCOME_RE
        else:
            balance_re = fp_types.BALANCE_RE
            cashflow_re = fp_types.CASHFLOW_RE
            income_re = fp_types.INCOME_RE

        if len(all_table_list) <2:
            raise exception_utils.NotableError
        unit_regex=re.compile("(\d+(,\d+)*)원")
        try:
            balancesheet_table,unit = self.parse_finance_table(
                balance_re,
                scrapyResponse,
                balance_data_re_list
            )
            table_style = self.parse_table_style_case(balancesheet_table)
            return_data['balance_sheet'] = {}
            return_data['balance_sheet']['table'] = balancesheet_table
            return_data['balance_sheet']['unit'] = unit
            return_data['balance_sheet']['style'] = table_style
        except exception_utils.TableStyleError as te: 
            error_message = f"No balancesheet data {link} " +str(ve) 
            logger.error(error_message)
            raise NotableError(error_message)
        except ValueError as ve:
            logger.error(f"No balancesheet data {link} " +str(ve) )
            raise exception_utils.NotableError
        except Exception as e:
            logger.error(f"No balancesheet data unexpected {link} "  +str(e))
            raise exception_utils.NotableError


        try:
            income_table,unit = self.parse_finance_table(
                income_re,
                scrapyResponse,
                income_data_re_list
            )
            table_style = self.parse_table_style_case(income_table)
            return_data['income_statement'] = {}
            return_data['income_statement']['table'] = income_table
            return_data['income_statement']['unit'] = unit
            return_data['income_statement']['style'] = table_style
        except exception_utils.TableStyleError as te: 
            error_message = f"No income statement data {link} " +str(ve) 
            logger.error(error_message)
            raise NotableError(error_message)
        except ValueError as ve:
            logger.error("No income statement data" + link)
            raise exception_utils.NotableError

        try:
            cashflow_table,unit = self.parse_finance_table(
                cashflow_re,
                scrapyResponse,
                cashflow_data_re_list
            )
            table_style = self.parse_table_style_case(cashflow_table)
            return_data['cashflow'] ={}
            return_data['cashflow']['table'] = cashflow_table
            return_data['cashflow']['unit'] = unit
            return_data['cashflow']['style'] = table_style
        except Exception as e:
            pass
        try:
            summary_table,unit = self.parse_finance_table(extra_re,scrapyResponse,summary_re_list)
            table_style = self.parse_table_style_case(summary_table)
            return_data['summary'] ={}
            return_data['summary']['table'] = summary_table
            return_data['summary']['unit'] = unit
            return_data['summary']['style'] = table_style
        except Exception:
            pass
        return return_data

    def parse_row_data_with_re(self,root,key,unit,is_ascending=True,data_key="", style=None):
        is_minus_data = False
        if data_key in ['net_income', 'operational_income']:
            xpath_expression = f"//tr//*[re:match(text(),'{key}')]/text()"
            xpath_result = root.xpath(xpath_expression, namespaces={'re': 'http://exslt.org/regular-expressions'})
            if xpath_result:
                row_text = xpath_result[0]
                if re.search('손[\s]*실', row_text) and not re.search('이[\s]*익', row_text):
                    is_minus_data = True

        odd_style_result = root.xpath(
            (
                f"//thead/tr[1]/td[4]/../../tr[1]/td[3]/../../..//*[re:match(text(),'{key}')]/ancestor::tr"
                f"|//thead/tr[1]/th[4]/../../..//tbody/tr[1]/td[7]/../..//*[re:match(text(),'{key}')]/ancestor::tr"
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
                for idx,td in enumerate(td_results[start_number:end_number]):
                    number_string = td.text_content().encode().decode()
                    try:
                        value = find_value(number_string,unit,data_key=data_key, is_minus_data=is_minus_data)
                        return value 
                    except Exception:
                        pass

        result = root.xpath(f"//tr//*[re:match(text(),'{key}')]/ancestor::tr", namespaces={'re': 'http://exslt.org/regular-expressions'})
        if result:
            td_results = result[0].xpath('.//td')
            values_td = td_results[1:] if is_ascending else td_results[:0:-1]
            for idx,td in enumerate(values_td):
                if idx == 0 and style == TABLE_CASE_WITH_NOTE:
                    continue
                number_string = td.text_content().encode().decode()
                try:
                    value = find_value(number_string,unit,data_key=data_key, is_minus_data=is_minus_data)
                    return value 
                except Exception as e:
                    pass
        raise ValueError(f"No data cannot find {key}")

    def parse_last_year_row(self,root,key,unit,is_ascending=True,data_key="", style=None):
        is_minus_data = False
        if data_key in ['net_income', 'operational_income']:
            xpath_expression = f"//tr//*[re:match(text(),'{key}')]/text()"
            xpath_result = root.xpath(xpath_expression, namespaces={'re': 'http://exslt.org/regular-expressions'})
            row_text = xpath_result[0]
            if re.search('손[\s]*실', row_text) and not re.search('이[\s]*익', row_text):
                is_minus_data = True
        odd_style_result = root.xpath(f".//thead/tr[1]/th[4]/../../tr[1]/th[3]/../../..//*[re:match(text(),'{key}')]/ancestor::tr", namespaces={'re': 'http://exslt.org/regular-expressions'})
        if odd_style_result:
            td_results = odd_style_result[0].xpath('.//td')
            if len(td_results) == 7:
                for idx,td in enumerate(td_results[3:5]):
                    number_string = td.text_content().encode().decode()
                    try:
                        value = find_value(number_string,unit,data_key=data_key, is_minus_data=is_minus_data)
                        return value 
                    except Exception as e:
                        pass
        result = root.xpath(f"//tr//*[re:match(text(),'{key}')]/ancestor::tr", namespaces={'re': 'http://exslt.org/regular-expressions'})
        if result:
            td_results = result[0].xpath('.//td')
            values_td = td_results[2:] if is_ascending else td_results[-2:0:-1]
            for idx,td in enumerate(values_td):
                if idx == 0 and style == TABLE_CASE_WITH_NOTE:
                    continue
                number_string = td.text_content().encode().decode()
                try:
                    value = find_value(number_string,unit,data_key=data_key, is_minus_data=is_minus_data)
                    return value 
                except Exception as e:
                    pass
        raise ValueError(f"No data cannot find {key}")
    def parse_table_style_case(self, root):
        root = LH.fromstring(root)
        # 데이터에 주석이 있는지 확인하기
        note_re = '주[\s]*석'

        seconds_head_rows = root.xpath(f".//thead/tr/th[2]/text()")
        if seconds_head_rows:
            if re.match(note_re,seconds_head_rows[0]):
                return TABLE_CASE_WITH_NOTE
        all_rows = root.xpath('.//tbody//tr')
        if len(all_rows) >3 and len(all_rows[0].getchildren())>=2:
            return TABLE_CASE_FIRST
        elif len(all_rows) ==1 and len(all_rows[0].getchildren())>=2:
            return TABLE_CASE_SECOND
        elif len(all_rows) ==2 and len(all_rows[0].getchildren())>=2:
            return TABLE_CASE_THIRD
        else:
            raise exception_utils.TableStyleError
    
    def parse_table_data(self, root, unit, data_dict, style=TABLE_CASE_FIRST):
        final_data_dict = {}
        #header를 parsing해서 가장 최근 데이터만 가져옴
        all_rows = root.xpath('.//tbody//tr')
        
        if style==TABLE_CASE_FIRST or style==TABLE_CASE_WITH_NOTE:
            header_list = root.xpath('.//thead//th')
            header_list = header_list[1:]
            ascending = self.check_is_ascending(header_list)
            for key in data_dict:
                try:
                    if not final_data_dict.get(key):
                        value = self.parse_row_data_with_re(root,data_dict[key],unit,ascending,data_key=key, style=style)
                        final_data_dict[key] = value
                except ValueError:
                    continue
            all_rows = root.xpath('//tr')
            for row in all_rows:
                td_list = row.xpath('.//td')
                if len(td_list) >= 2:
                    td_key = td_list[0].text_content().strip().replace('.', '')
                    number_string = td_list[1].text_content().strip() if ascending else td_list[-1].text_content().strip()
                    try:
                        if not final_data_dict.get(td_key):
                            final_data_dict[td_key] = find_value(number_string,unit,td_key=td_key)
                    except Exception as e:
                        pass

        return final_data_dict

    def return_remained_petovski_data(self,result,data_dict):
        re_key_list = set(data_dict.keys())
        inserted_data_keys = set(result.keys())
        remained_keys = re_key_list -inserted_data_keys
        return remained_keys

    def fill_insufficient_petovski_data(self,result,data_dict,root,unit):
        remained_keys = self.return_remained_petovski_data(result,data_dict)
        for key in remained_keys:
            try:
                value = self.parse_row_data_with_re(root,data_dict[key],unit,data_key=key)
                result[key] = value
            except:
                pass
        return result 

    def fillin_insufficient_balance_data(self, balance_data_result):
        if balance_data_result.get("book_value") is None:
            if balance_data_result.get("total_assets") and balance_data_result.get("total_debt"):
                balance_data_result['book_value'] = balance_data_result['total_assets'] - balance_data_result['total_debt']
        if balance_data_result.get('current_debt') is None:
            if balance_data_result.get("total_debt") and balance_data_result.get("longterm_debt"):
                balance_data_result['current_debt'] = balance_data_result['total_debt'] - balance_data_result['longterm_debt']
        return balance_data_result
            


        pass
        # if remained_keys.get("total_assets"):
        #     pass
        
    def parse_balancesheet_table(self,table,unit,table_style):
        root = LH.fromstring(table)
        result = self.parse_table_data(root,unit,petovski_balancesheet_dict,style=table_style)

        if not result.get("current_assets"):
            current_assets = 0
            current_debt = 0 
            total_assets = result.get("total_assets") 
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
            result['current_debt'] = current_debt if not result.get('current_debt') else result.get('current_debt')
            result['longterm_debt'] = total_debt - result['current_debt']
        return result 

    def parse_cashflow_table(self,table,unit,table_style):
        root = LH.fromstring(table)
        result = self.parse_table_data(root,unit,petovski_cash_dict,style=table_style)
        return result


    def parse_incomestatement_table(self,table,unit,table_style):
        root = LH.fromstring(table)
        result = self.parse_table_data(root,unit,petovski_income_statement_dict,style=table_style)
        if result.get("sales") is not None and result['sales'] ==0:
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
                    if re.search(petovski_sub_income_dict['cost_of_goods_sold'],key):
                        cogs = original_dict[key]
                        result['gross_profit'] = sales - cogs        
        if  'extra_ordinary_loss' not in result:
            result['extra_ordinary_loss'] = 0
        if 'operational_income' not in result:
            try:
                operational_sales = self.parse_row_data_with_re(root,petovski_sub_income_dict['operational_sales'],unit,data_key='operational_sales',style=table_style)
                operational_costs = self.parse_row_data_with_re(root,petovski_sub_income_dict['operational_costs'],unit,data_key='operational_sales',style=table_style)
                result['operational_income'] = operational_sales - operational_costs
            except ValueError:
                pass
        #매출 총이익이 없을 경우 영업이익으로 이를 대체
        if 'gross_profit' not in result:
            result['gross_profit'] = result['operational_income']
        return result 


    
    def parse_report_link(self,link,soup,is_connected_inside_normal=False):
        try:
            table_dict = self.return_financial_report_table(link,soup,is_connected_inside_normal=is_connected_inside_normal)
        except exception_utils.NotableError:
            raise exception_utils.ExpectedError 
        except Exception as e:
            logger.error(f"Unexpected error while parsing report table link {link} " + str(e))
            raise ValueError

        result = {}
        if table_dict.get('cashflow') and table_dict['cashflow']['style'] in [
            TABLE_CASE_FIRST, TABLE_CASE_WITH_NOTE
        ]:
            try:
                cashflow_result = self.parse_cashflow_table(
                    table_dict['cashflow']['table'],
                    table_dict['cashflow']['unit'],
                    table_dict['cashflow']['style']
                )
            except exception_utils.TableStyleError:
                logger.error(f"Unexpected cashflow table {link}")
                raise exception_utils.ExpectedError

        else:
            cashflow_result = {}
        
        try:
            if table_dict.get('income_statement') and table_dict['income_statement']['style'] in [
                TABLE_CASE_FIRST, TABLE_CASE_WITH_NOTE
            ]:
                incomestatement_result = self.parse_incomestatement_table(
                    table_dict['income_statement']['table'], 
                    table_dict['income_statement']['unit'],
                    table_dict['income_statement']['style']
                )
            else:
                incomestatement_result = {}
        except exception_utils.TableStyleError:
            logger.error(f"Unexpected income statement table {link}")
            raise exception_utils.ExpectedError
        try:
            if table_dict.get('balance_sheet') and table_dict['balance_sheet']['style'] in [
                TABLE_CASE_FIRST, TABLE_CASE_WITH_NOTE
            ]:

                balachesheet_result = self.parse_balancesheet_table(
                    table_dict['balance_sheet']['table'],
                    table_dict['balance_sheet']['unit'],
                    table_dict['balance_sheet']['style'],
                )
            else:
                balachesheet_result = {}
        except exception_utils.TableStyleError:
            logger.error(f"Unexpected balancesheet table {link}")
            raise exception_utils.ExpectedError

        if table_dict.get('summary') and table_dict['summary']['style'] in [
                TABLE_CASE_FIRST, TABLE_CASE_WITH_NOTE
            ]:
            root = LH.fromstring(table_dict['summary']['table'])
            unit = table_dict['summary']['unit']
            balachesheet_result = self.fill_insufficient_petovski_data(balachesheet_result,petovski_balancesheet_dict,root,unit)
            incomestatement_result = self.fill_insufficient_petovski_data(incomestatement_result,petovski_income_statement_dict,root,unit)
            cashflow_result = self.fill_insufficient_petovski_data(cashflow_result,petovski_cash_dict,root,unit)

        if not incomestatement_result.get('net_income') and cashflow_result.keys():
            for key in cashflow_result:
                if re.search(petovski_income_statement_dict['net_income'],key):
                    incomestatement_result['net_income'] = cashflow_result[key]

        if not table_dict.get('cashflow') and table_dict['income_statement']['style'] == TABLE_CASE_FIRST :
            root = LH.fromstring(table_dict['income_statement']['table'])
            try:
                operational_income = incomestatement_result['operational_income']
                depreciation_cost = self.parse_row_data_with_re(root,petovski_sub_income_dict['depreciation_cost'],table_dict['income_statement']['unit'])
                tax_costs = self.parse_row_data_with_re(root,petovski_sub_income_dict['corporate_tax_cost'],table_dict['income_statement']['unit'])
                try:
                    current_assets = balachesheet_result.get('current_assets')
                    current_debt = balachesheet_result.get('current_debt')
                    current_working_capital = current_assets - current_debt
                    balance_elem = LH.fromstring(table_dict['balance_sheet']['table'])
                    last_year_ca = self.parse_last_year_row(
                        balance_elem,
                        petovski_balancesheet_dict['current_assets'],
                        table_dict['balance_sheet']['unit'],
                        data_key='current_assets'
                    )
                    last_year_cb = self.parse_last_year_row(
                        balance_elem,
                        petovski_balancesheet_dict['current_debt'],
                        table_dict['balance_sheet']['unit'],
                        data_key='current_debt'
                    )
                    cashflow_result['last_year_current_assets'] = last_year_ca
                    cashflow_result['last_year_current_debt'] = last_year_cb
                    last_year_working_capital = last_year_ca - last_year_cb
                    working_capital_change = current_working_capital - last_year_working_capital
                except Exception as e:
                    print(e)
                    working_capital_change = 0
                cashflow_result['cashflow_from_operation'] = operational_income + depreciation_cost - tax_costs + working_capital_change
                cashflow_result['direct_cashflow'] = False
            except Exception as e:
                logger.error(f"Error occur while parsing no cashtable data {link}" + str(e))
                raise exception_utils.ExpectedError
        result.update(cashflow_result)
        result.update(balachesheet_result)
        result.update(incomestatement_result)
        cash_remained_keys = self.return_remained_petovski_data(result,petovski_cash_dict)
        balancesheet_remained_keys = self.return_remained_petovski_data(result,petovski_balancesheet_dict)
        incomestatement_remained_keys = self.return_remained_petovski_data(result,petovski_income_statement_dict)
        if cash_remained_keys or balancesheet_remained_keys or incomestatement_remained_keys:
            if table_dict['balance_sheet']['style'] in [TABLE_CASE_SECOND,TABLE_CASE_THIRD] \
            or table_dict['cashflow']['style'] in [TABLE_CASE_SECOND,TABLE_CASE_THIRD] \
            or table_dict['income_statement']['style'] in [TABLE_CASE_SECOND,TABLE_CASE_THIRD]:
                driver_result = self.return_driver_report_data(link,table_dict,result)
                result.update(driver_result)
        result = self.fillin_insufficient_balance_data(result)
        cash_remained_keys = self.return_remained_petovski_data(result,petovski_cash_dict)
        balancesheet_remained_keys = self.return_remained_petovski_data(result,petovski_balancesheet_dict)
        incomestatement_remained_keys = self.return_remained_petovski_data(result,petovski_income_statement_dict)

        if cash_remained_keys:
            logger.error(f"Not Sufficient cashflow with url {link} {cash_remained_keys}")
            raise exception_utils.ExpectedError
        if balancesheet_remained_keys:
            logger.error(f"Not Sufficient balancesheet with url {link} {balancesheet_remained_keys}")
            raise exception_utils.ExpectedError
        if incomestatement_remained_keys:
            logger.error(f"Not Sufficient income statements with url {link} {incomestatement_remained_keys}",)
            raise exception_utils.ExpectedError
        return result

    def return_multiple_link_results(self,link_list,jump=100):
        success_list = []
        failed_list = []
        for i in range(0,len(link_list),jump):
            end = i + jump
            result = self.return_async_func_results('return_reportlink_data',link_list[i:end],use_callback=False)
            for s,f in result:
                success_list.extend(s)
                failed_list.extend(f)
        return {'success_list':success_list, 'failed_list':failed_list}

    async def parse_excel_download_link(self,soup):
        download_soup = soup.find(title="다운로드")
        if not download_soup:
            raise exception_utils.NoDataError

        download_string = download_soup.parent.get('onclick')
        search_download_link = re.search("\('(\d+)'\,.*'(\d+)'\)",download_string)
        if not search_download_link:
            raise exception_utils.NoDataError
        rcpNo = search_download_link.group(1)
        downloadNo = search_download_link.group(2)
        download_link =f'http://dart.fss.or.kr/pdf/download/main.do?rcp_no={rcpNo}&dcm_no={downloadNo}'
        download_soup = await self.return_async_get_soup(download_link)
        statements_tag = download_soup.find('td',text=re.compile("재무제표"))
        if not statements_tag:
            raise exception_utils.NoDataError
        report_link = statements_tag.parent.find('a').get('href')
        total_link = 'http://dart.fss.or.kr' + report_link 
        return total_link

    def parse_excel_file_table(self,sheet_name_list):
        table_re_list = {
            'balance_sheet':fp_types.BALANCE_RE,
            'income_statement':fp_types.INCOME_RE,
            'cashflow':fp_types.CASHFLOW_RE
        }
        data = {}
        for idx,sheet_name in enumerate(sheet_name_list):
            for (table_name,search_re) in table_re_list.items():
                if re.search(search_re,sheet_name) and '포괄' not in sheet_name:
                    is_connected = '연결' in sheet_name
                    table_key = table_name + '_connected' if is_connected else table_name
                    data[table_key] = idx

        if not data.get('income_statement'):
            for idx,sheet_name in enumerate(sheet_name_list):
                if re.search(table_re_list['income_statement'],sheet_name):
                    is_connected = '연결' in sheet_name
                    table_key = 'income_statement' + '_connected' if is_connected else table_name
                    data[table_key] = idx
        return data

    def parse_balancesheet_sheet(self,wb,sheet_idx):
        result = self.return_parsed_sheet_data(wb,sheet_idx,petovski_balancesheet_dict)
        return result 


    def parse_cashflow_sheet(self,wb,sheet_idx):
        result = self.return_parsed_sheet_data(wb,sheet_idx,petovski_cash_dict)
        return result


    def parse_incomestatement_sheet(self,wb,sheet_idx):
        result = self.return_parsed_sheet_data(wb,sheet_idx,petovski_income_statement_dict)
        if result.get("sales") is not None and result['sales'] ==0:
            result.pop('sales')
        if 'extra_ordinary_profit' not in result:
            result['extra_ordinary_profit'] = 0 
        if  'extra_ordinary_loss' not in result:
            result['extra_ordinary_loss'] = 0
        if 'operational_income' not in result:
            try:
                operational_sales = None 
                operational_income = None
                for key in result:
                    sales_search = re.search(petovski_sub_income_dict['operational_sales'],key)
                    if sales_search:
                        operational_sales = result[key]
                    cost_search = re.search(petovski_sub_income_dict['operational_costs'],key)
                    if cost_search:
                        operational_income = result[key]
                    if operational_sales is not None and operational_costs is not None:
                        result['operational_income'] = operational_sales - operational_costs
            except ValueError:
                pass
        if 'gross_profit' not in result:
            if result.get('sales'):
                sales = result['sales']
                original_dict = copy.deepcopy(result)
                for key in original_dict:
                    if re.search(petovski_sub_income_dict['cost_of_goods_sold'],key):
                        cogs = original_dict[key]
                        result['gross_profit'] = sales - cogs
        return result 



    def parse_sheet_table(self,sheet,re_dict,unit):
        data = {}
        for i in range (sheet.nrows) :
            row = sheet.row_values(i)
            if len(row)>=2 and type(row[0]) is str and (type(row[1]) is int or type(row[1]) is float):
                is_pitovski_data = False
                for re_key in re_dict.keys():
                    re_val = re_dict[re_key]
                    if re.search(re_val,row[0]) and not data.get(re_key):
                        data[re_key] = row[1] * unit
                        is_pitovski_data = True
                        break
                if  not is_pitovski_data:
                    row_key = row[0].strip().replace('.', '')
                    data[row_key] = row[1]
        return data

    def parse_sheet_unit(self,sheet):
        lines = []
        columns = []
        data = {}
        for i in range (sheet.nrows) :
            row = sheet.row_values(i)
            for j in range(len(row)) :
                if type(row[j]) is str and '단위' in row[j]:
                    return self.parse_unit_string(row[j])                
        return 1

    def return_parsed_sheet_data(self,wb,sheet_idx,petovski_dict):
        sheet = wb.sheet_by_index(sheet_idx)
        sheet_unit = self.parse_sheet_unit(sheet)
        sheet_data = self.parse_sheet_table(sheet,petovski_dict,sheet_unit)
        return sheet_data

    def return_final_sheet_table_data(self,wb,table_data,is_connected=False):
        final_result = {}
        balance_idx = table_data['balance_sheet_connected'] if is_connected else table_data['balance_sheet']
        income_idx = table_data['income_statement_connected'] if is_connected else table_data['income_statement']
        cash_idx = table_data['cashflow_connected'] if is_connected else table_data['cashflow']
        balance_sheet_result = self.parse_balancesheet_sheet(wb,balance_idx)
        incomestatement_result = self.parse_incomestatement_sheet(wb,income_idx)
        cashflow_result = self.parse_cashflow_sheet(wb,cash_idx)
        if not cashflow_result.keys():
            depreciation_costs = None 
            tax_costs = None
            operational_income = incomestatement_result.get('operational_income')
            for key in incomestatement_result:
                depreciation_search = re.search(petovski_sub_income_dict['depreciation_cost'],key)
                if depreciation_search:
                    depreciation_costs = incomestatement_result[key]
                tax_search = re.search(petovski_sub_income_dict['corporate_tax_cost'],key)
                if tax_search:
                    tax_costs = incomestatement_result[key]
                if operational_income is not None and depreciation_costs is not None and tax_costs is not None:
                    cashflow_result['cashflow_from_operation'] = operational_income + deperciation_cost - tax_costs
        
        cash_remained_keys = self.return_remained_petovski_data(cashflow_result,petovski_cash_dict)
        if cash_remained_keys:
            raise exception_utils.NotSufficientError("cashflow")
        balancesheet_remained_keys = self.return_remained_petovski_data(balance_sheet_result,petovski_balancesheet_dict)
        if balancesheet_remained_keys:
            raise exception_utils.NotSufficientError("balancesheet")
        incomestatement_remained_keys = self.return_remained_petovski_data(incomestatement_result,petovski_income_statement_dict)
        if incomestatement_remained_keys:
            raise exception_utils.NotSufficientError("income statement")
        final_result.update(cashflow_result)
        final_result.update(balance_sheet_result)
        final_result.update(incomestatement_result)
        return final_result

    def parse_excel_file_data(self,fname,**kwargs):
        wb = xlrd.open_workbook(fname)
        sheet_name_list = wb.sheet_names()
        table_data = self.parse_excel_file_table(sheet_name_list)
        normal_table_must_list = ['balance_sheet','income_statement','cashflow']
        connected_table_must_list = ['balance_sheet_connected','income_statement_connected','cashflow_connected']
        is_normal_info = all([ i in table_data.keys() for i in normal_table_must_list])
        is_connected_info = all([ i in table_data.keys() for i in connected_table_must_list])
        result = []
        if is_normal_info:
            final_result = self.return_final_sheet_table_data(wb,table_data,is_connected=False)
            final_result.update(kwargs)
            final_result['report_type'] = NORMAL_FINANCIAL_STATEMENTS
            result.append(final_result)
        if is_connected_info:
            final_result = self.return_final_sheet_table_data(wb,table_data,is_connected=True)
            final_result.update(kwargs)
            final_result['report_type'] = CONNECTED_FINANCIAL_STATEMENTS
            result.append(final_result)
        return result

    async def return_excel_table_data(self,soup,**kwargs):
        try:
            link = await self.parse_excel_download_link(soup)
        except exception_utils.NoDataError:
            raise exception_utils.ExpectedError
        random_file_name =''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10)) + '.xls'
        report_link = kwargs.get("report_link",'')
        async with self.session.get(link,headers=headers,allow_redirects=True) as resp:
            if resp.status != 200:
                logger.error(f"Request Error on {report_link} {link}")
                raise exception_utils.ExpectedError
            with  open(random_file_name, 'wb') as f:
                try:
                    content = await resp.read()
                    f.write(content)
                except Exception as e:
                    logger.error(f"Error saving error on {report_link} {link} {str(e)}")
                    raise exception_utils.ExpectedError
        try:
            result = self.parse_excel_file_data(random_file_name,excel_link=link,**kwargs)
        except exception_utils.NotSufficientError as e:
            logger.error(f"Error not sufficinet {str(e)} table {report_link} {link}")
            raise exception_utils.ExpectedError
        os.remove(random_file_name)
        return result

    def return_update_dict(self,default_dict,update_result):
        base = copy.deepcopy(default_dict)
        base.update(update_result)
        return base

        
    async def return_reportlink_data(self,link="",code="",reg_date=None,corp_name="",market_type="",title="",period_type=YEARLY_REPORT,reporter=""):
        total_result = []
        failed_result = []
        is_connected_data_made = False
        is_normal_data_made = False
        default_dict = {
            'report_link':link,
            'code':code,
            'corp_name':corp_name, 
            'period_type':period_type,
            'reg_date':reg_date,
            'market_type':market_type
        }
        try:
            soup = await self.return_async_get_soup(link)
            if not soup:
                logger.error(f"Error occured while no soup {link}")
                return []
        except ValueError:
            logger.error(f"Error occured while return soup {link}")
            return []
        try:
            stock_count_link = ""
            stock_count_link = parse_stockcount_section_link(soup)
            stockcount_soup = await self.return_async_get_soup(stock_count_link)
            stock_count_data = return_stockcount_data(link, stockcount_soup)
        except Exception as e: 
            logger.error(
                f"Error while parsing stock count {link} {stock_count_link} " + str(e)
            )
            stock_count_data = {}

        try:
            result = await self.return_excel_table_data(soup,report_link=link,code=code,reg_date=reg_date,corp_name=corp_name,market_type=market_type,title=title,period_type=period_type,reporter=reporter)
            for r in result:
                r.update(stock_count_data)
            if len(result) ==2:
                return result,failed_result
            if len(result) ==1:
                total_result.append(result[0])
                if result[0]['report_type'] == NORMAL_FINANCIAL_STATEMENTS:
                    is_normal_data_made= True
                else:
                    is_connected_data_made=True
        except exception_utils.ExpectedError:
            pass
        except Exception as e:
            logger.error(f"Unexpected Error parsing excel data {str(e)} {link}")


        try:
            link_data = self.parse_financial_section_link(soup)
        except exception_utils.ReportLinkParseError:
            if len(total_result)==0:
                logger.error(f"No data exists in link {link}")
            logger.error(f"Error occured while return financial section {link} ")
            failed_info = self.return_update_dict(default_dict,{})
            failed_result.append(failed_info)
            return total_result,failed_result


        normal_table_link = link_data.get('link_fs')
        connected_table_link = link_data.get('link_connected_fs')
        if not normal_table_link:
            logger.error(f"Parse financial section job cannot get normal table link {link}")
            if len(total_result)==0:
                logger.error(f"No data exists in link {link}")
            failed_info = self.return_update_dict(default_dict,{})
            return total_result,failed_result

        # 연결재무재표가 일반재무재표와 같은페이지에있는경우 이를확인시킨다.
        is_connected_inside_normal =  False
        if not connected_table_link:
            connected_table_link = normal_table_link
            is_connected_inside_normal = True



        get_table_tasks  = []
        if normal_table_link and not is_normal_data_made:
            get_table_tasks.append(
                (
                    NORMAL_FINANCIAL_STATEMENTS,
                    normal_table_link,
                    self.return_async_get_soup(normal_table_link)
                )
            )  
        if connected_table_link and not is_connected_data_made:
            get_table_tasks.append(
                (
                    CONNECTED_FINANCIAL_STATEMENTS,
                    connected_table_link,
                    self.return_async_get_soup(connected_table_link)
                )
            )
        # Excel parsing 이후 table 결과값이 존재하지 않으면 결과 반환
        if not get_table_tasks:
            if len(total_result) == 1:
                failed_dict = {'table_link': normal_table_link}\
                         if is_connected_data_made else {'table_link': connected_table_link}
                failed_info = self.return_update_dict(default_dict,failed_dict)
                failed_result.append(failed_info)
                return total_result,failed_result   
            else:
                failed_info = self.return_update_dict(default_dict,{})
                failed_result.append(failed_info)
                return total_result,failed_result   

        soup_list = await asyncio.gather(*[x[2] for x in get_table_tasks], return_exceptions=True)
        for idx,soup in enumerate(soup_list):
            try:
                report_type = get_table_tasks[idx][0]
                table_link = get_table_tasks[idx][1]
                if idx == 1 and is_connected_inside_normal:
                    result = self.parse_report_link(table_link,soup,is_connected_inside_normal=True)
                else:
                    result = self.parse_report_link(table_link,soup,is_connected_inside_normal=False)
                result['report_link'] = link
                result['table_link'] = table_link
                result['code'] = code
                result['corp_name'] = corp_name
                result['report_type'] = report_type
                result['period_type'] = period_type
                result['reg_date'] = reg_date
                result['market_type'] = market_type
                result.update(stock_count_data)
                total_result.append(result)
            except exception_utils.ExpectedError:
                failed_dict = {'report_link':link,'table_link':table_link,'report_type':report_type}
                failed_info = self.return_update_dict(default_dict,failed_dict)
                failed_result.append(failed_info)
                pass
            except Exception as e:
                logger.error(f"Unexpected error occured while parsing table "+str(e)+table_link)
                failed_dict = {'report_link':link,'table_link':table_link,'report_type':report_type}
                failed_info = self.return_update_dict(default_dict,failed_dict)
                failed_result.append(failed_info)
        return total_result,failed_result

    def return_company_report_link_list(self,code,company_name,report_type,start_date=None):
        final_results = []
        currentPage = 1
        while True: 
            try:
                if currentPage>1000:
                    break
                result = self.parse_single_page_data(code,currentPage,report_type,company_name,start_date=start_date,textCrpCik=None)
                if not result:
                    newcode = ''.zfill(6-len(code)) + code
                    result = self.parse_single_page_data(newcode,currentPage,report_type,company_name,start_date=start_date,textCrpCik=None)
                currentPage += 1
                if not result:
                    break 
                final_results.extend(result)
            except Exception as e:
                break 
        return final_results
    def return_company_report_data_list(self,code,company,start_date=None):
        try:
            report_list = self.return_company_report_link_list(code,company, fp_types.YEARLY_REPORT,start_date=start_date)
        except Exception as e: 
            errorMessage = f"Error occured while return report link list {code},{company} "+str(e)
            logger.error(errorMessage)
            raise ValueError(errorMessage)
        try:
            quarter_list = self.return_company_report_link_list(code,company, fp_types.QUARTER_REPORT,start_date=start_date)
            report_list.extend(quarter_list)
        except Exception as e: 
            errorMessage = f"Error occured while return report link list {code},{company} "+str(e)
            logger.error(errorMessage)
            raise ValueError(errorMessage)
        data_list = self.return_multiple_link_results(report_list)
        return data_list


    def parse_financial_section_link(self,soup):
        head_lines = soup.find('head').text.split("\n")
        hangul = re.compile('[^ ㄱ-ㅣ가-힣]+')
        re_tree_find1 = re.compile("[^\W\d_]")
        re_tree_find1_bak = re.compile("재무제표$")
        re_tree_find1_bak2 = re.compile("(?!연결)")

        line_num = 0
        line_find = 0
        link_connected_fs= None
        link_fs=None
        return_data = {}
        for idx,head_line in enumerate(head_lines):
            hangul = " ".join(re.split('[^ ㄱ-ㅣ가-힣]+', head_line))
            hangul = hangul.replace(" ",'').replace('등','').replace('\n','').replace(' ','').strip()
            if '재무제표' == hangul and not link_fs:
                link_fs = get_financial_statesments_links(soup,head_lines,idx)
            if '연결재무제표' == hangul and not link_connected_fs:
                link_connected_fs =  get_financial_statesments_links(soup,head_lines,idx)

        if not link_connected_fs and not link_fs:
            for idx,head_line in enumerate(head_lines):
                hangul = " ".join(re.split('[^ ㄱ-ㅣ가-힣]+', head_line))
                hangul = hangul.replace(" ",'').replace('등','').replace('\n','').replace(' ','').strip()
                if '재무제표' in hangul and not link_fs:
                    link_fs = get_financial_statesments_links(soup,head_lines,idx)
                if '연결재무제표' in hangul and not link_connected_fs:
                    link_connected_fs =  get_financial_statesments_links(soup,head_lines,idx)
        if not link_connected_fs and not link_fs:
            raise exception_utils.ReportLinkParseError
        else:
            data = {}
            data['link_connected_fs'] = link_connected_fs 
            data['link_fs'] = link_fs 
            return data 


    def return_driver_report_data(self,link,table_dict,result={}):
        try:
            driver = return_driver()
            driver.get(link)
        except Exception as e: 
            raise ValueError(f"Error connect to link with driver {link}")

        matching_table_list = driver.find_elements_by_xpath("//p/following-sibling::table/tbody[count(descendant::tr)=1 or count(descendant::tr)=2]/..")
        if table_dict.get('cashflow'):
            try:
                cash_remained_keys = self.return_remained_petovski_data(result,petovski_cash_dict)
                if cash_remained_keys:
                    cashflow_table = self.return_driver_matching_table(
                        matching_table_list,
                        cashflow_data_re_list
                    )
                    cashflow_result = self.parsing_driver_table_data(
                        driver,
                        cashflow_table,
                        petovski_cash_dict,
                        table_dict['cashflow']['unit']
                    )
                else:
                    cashflow_result = {}
            except Exception as e:
                error_message = f"Error while making driver cashflow table {link} " + str(e)
                logger.error(error_message)
                raise ValueError(error_message)

        else:
            cashflow_result = {}
        try:
            remained_income_keys = self.return_remained_petovski_data(result, petovski_income_statement_dict)
            if remained_income_keys:
                income_table = self.return_driver_matching_table(
                    matching_table_list,
                    income_data_re_list
                )
                incomestatement_result = self.parsing_driver_table_data(
                    driver,
                    income_table,
                    petovski_income_statement_dict,
                    table_dict['income_statement']['unit']
                )
            else:
                incomestatement_result = {}
        except Exception as e:
            error_message = f"Error while making driver income table {link} " + str(e)
            logger.error(error_message)
            raise ValueError(error_message)
        try:
            remained_balance_keys = self.return_remained_petovski_data(result, petovski_balancesheet_dict)
            if remained_balance_keys:
                balance_table = self.return_driver_matching_table(
                    matching_table_list,
                    balance_data_re_list
                )
                balachesheet_result = self.parsing_driver_table_data(
                    driver,
                    balance_table,
                    petovski_balancesheet_dict,
                    table_dict['balance_sheet']['unit'],
                    additional_table_data=fp_types.additional_balance_data
                )
            else:
                balachesheet_result = {}
        except exception_utils.TableStyleError:
            error_message = f"Error while making driver balance table {link} " + str(e)
            logger.error(error_message)
            raise ValueError(error_message)

        if incomestatement_result and not incomestatement_result.get('extra_ordinary_loss'):
            incomestatement_result['extra_ordinary_loss'] = 0 
        if incomestatement_result and not incomestatement_result.get('extra_ordinary_profit'):
            incomestatement_result['extra_ordinary_profit'] = 0 

        if not incomestatement_result.get('net_income') and cashflow_result.keys():
            cashflow_table = self.return_driver_matching_table(
                matching_table_list,
                cashflow_data_re_list
            )
            netincome_result = self.parsing_driver_table_data(
                driver,
                cashflow_table,
                {'net_income': petovski_income_statement_dict['net_income']},
                table_dict['balance_sheet']['unit'],
                insert_span_needed=False
            )
            incomestatement_result['net_income'] = netincome_result.get('net_income')


        if not table_dict.get('cashflow'):
            try:
                income_sub_result = self.parsing_driver_table_data(
                    driver,
                    income_table,
                    {
                        'depreciation_cost': petovski_sub_income_dict['depreciation_cost'],
                        'corporate_tax_cost': petovski_sub_income_dict['corporate_tax_cost'],
                    },
                    table_dict['balance_sheet']['unit'],
                    insert_span_needed=False
                )
                operational_income = incomestatement_result['operational_income']
                depreciation_cost = income_sub_result['depreciation_cost']
                corporate_tax_cost = income_sub_result['corporate_tax_cost']
                cashflow_result['cashflow_from_operation'] = operational_income + depreciation_cost - corporate_tax_cost
            except Exception as e:
                logger.error(f"Error occur while parsing no cashtable data {link}" + str(e))
                raise exception_utils.ExpectedError
        result.update(cashflow_result)
        result.update(balachesheet_result)
        result.update(incomestatement_result)
        return result

    def return_driver_matching_table(self, matching_table_list, re_list):
        for idx,table in enumerate(matching_table_list):
            reg_data = ' and '.join(re_list)  
            if re.search(reg_data, table.text):
                try:
                    return table
                except Exception as e: 
                    continue
        raise ValueError
        
    def parsing_driver_table_data(self, driver, table_elem, reg_data, unit, insert_span_needed=True,additional_table_data={}):
        result = {}
        td_list = table_elem.find_elements_by_xpath('.//td')
        for td in td_list:
            if not td.find_elements_by_xpath('.//span'):
                inner_html_string = '<br>'.join(['<span>'+x.strip()+'</span>' for x in td.get_attribute("innerHTML").split('<br>')])
                driver.execute_script(f"arguments[0].innerHTML='{inner_html_string}'", td)
        td_list = table_elem.find_elements_by_xpath('.//td')
        first_td = td_list[0]
        value_td_list = td_list[1:]
        value_span_list = [ td.find_elements_by_xpath('.//span') for td in  value_td_list]
        first_value_td = value_td_list[0]
        for span in first_td.find_elements_by_xpath('.//span'):
            span_text = span.text.strip()
            if span_text:
                is_re_caught = False       
                found_re = ''
                for table_re in reg_data:
                    if result.get(table_re, None) is None and re.search(reg_data[table_re],span_text):
                        location = span.location
                        location_y = location.get('y')
                        is_re_caught = True
                        found_re = table_re
                        break 
                if not is_re_caught:
                    for additional_re in additional_table_data:
                        if result.get(additional_re, None) is None and re.search(additional_table_data[additional_re],span_text):
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
                                    value = find_value(value_span.text,unit,data_key=found_re)
                                    result[found_re] = value
                                    value_found = True
                                    break
                                except Exception as e: 
                                    pass
                        if value_found:
                            break
        return result
        