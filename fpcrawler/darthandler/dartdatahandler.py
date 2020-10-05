import copy
from datetime import datetime as dt
import logging
import os
import random
import re
import string
from typing import List
import requests
import asyncio
from bs4 import BeautifulSoup
from utils import exception_utils
from utils.class_utils import DataHandlerClass    
from aiohttp import (
    ClientSession,
)
from aiosocksy.connector import (
    ProxyConnector, ProxyClientRequest
)
from fp_types import (
    YEARLY_REPORT, 
    QUARTER_REPORT,
    SEMINUAL_REPORT,
    MARCH_REPORT,
    SEPTEMBER_REPORT,
    CONNECTED_FINANCIAL_STATEMENTS, 
    NORMAL_FINANCIAL_STATEMENTS
)
# from utils.torrequest import reset_ip_address
from .dartdataparsehandler import (
    get_financial_statesments_links
)
from .dartreporthandler import DartReportHandler
from .dartexcelparsinghandler import DartExcelParser
import fp_types


logger = logging.getLogger()


class DartDataHandler(DataHandlerClass):

    def __init__(self, *args, **kwargs):
        user_agent = (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) '
            'AppleWebKit/602.2.14 (KHTML, like Gecko) '
            'Version/10.0.1 Safari/602.2.14'
        )
        self.headers = {
            'User-Agent': user_agent,
            'Accept': (
                'text/html,application/xhtml+xml,'
                'application/xml;q=0.9,image/webp,*/*;q=0.8'
            ),
            'Connection': 'keep-alive'
        }
        self.proxies = {'http': 'http:torproxy:9300'}
        self.sockproxy = 'socks5://torproxy:9200'
        
    async def return_async_get_soup(self, url, proxy_object=None, timeout=10):
        try:
            connector = ProxyConnector(remote_resolve=False)
            async with ClientSession(
                connector=connector,
                request_class=ProxyClientRequest
            ) as s:
                async with s.get(
                    url, 
                    headers=self.headers,
                    proxy=self.sockproxy
                ) as resp:
                    result_string = await resp.text()                    
                    soup = BeautifulSoup(result_string, 'html.parser')
                    return soup
        except Exception as e:
            print(e)
            raise ValueError(f"Error making async requests {url}" + str(e))

    def parse_linklist_data(
        self, code, currentPage, 
        report_type, company_name, 
        start_date=None, textCrpCik=None,
        original_code=None
    ):
        if not original_code:
            original_code = code
        today_string = dt.strftime(dt.now(), '%Y%m%d')
        start_date = start_date if start_date else '19980101'
        code = str(code)
        code = ''.zfill(6-len(code)) + code
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
        url = 'http://dart.fss.or.kr/dsab002/search.ax'
        result = requests.post(
            url, 
            data=post_data, 
            headers=self.headers,
            proxies=self.proxies
        )
        if result.status_code > 300:
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
            if len(tds) < 5:
                continue
            link = 'http://dart.fss.or.kr' + tds[2].a['href']
            date = tds[4].text.strip().replace('.', '-')
            corp_name = tds[1].text.strip()
            market = tds[1].img['title']
            title = " ".join(tds[2].text.split())
            reporter = tds[3].text.strip()
            data_object = {}
            data_object['code'] = str(original_code)
            data_object['link'] = link
            data_object['reg_date'] = date
            data_object['corp_name'] = corp_name
            data_object['market_type'] = market
            data_object['title'] = title
            if '03' in title and report_type == QUARTER_REPORT:
                data_object['period_type'] = MARCH_REPORT
            elif '09' in title and report_type == QUARTER_REPORT:
                data_object['period_type'] = SEPTEMBER_REPORT
            else:
                data_object['period_type'] = report_type
            data_object['reporter'] = reporter
            data_object_list.append(data_object)
        return data_object_list

    def parse_dart_search_result(
            self, soup: BeautifulSoup, code: str, report_type: str
    ):
        table = soup.find('table')
        if not table:
            return []
        trs = table.findAll('tr')
        tr_list = trs[1:]
        data_object_list = []
        for tr in tr_list:
            tds = tr.findAll('td')
            if len(tds) < 5:
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
        today_string = dt.strftime(dt.now(), '%Y%m%d')
        start_date = start_date if start_date else '19990101'
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
            data = requests.post(url, data=tpdata, proxies=self.proxies)
            if data.status_code == 200:
                if data.text != 'null':
                    post_data['textCrpCik'] = data.text.strip()
                else:
                    tpdata = {'textCrpNm': code}
                    data = requests.post(
                        url, 
                        data=tpdata, 
                        proxies=self.proxies
                    )
                    if data.text != 'null' and data.status_code == 200:
                        post_data['textCrpCik'] = data.text.strip()

        url = 'http://dart.fss.or.kr/dsab002/search.ax'
        result = requests.post(
            url, 
            data=post_data, 
            headers=self.headers,
            proxies=self.proxies
        )
        if result.status_code > 300:
            return []

        soup = BeautifulSoup(result.text, 'html.parser', from_encoding='utf-8')
        return self.parse_dart_search_result(soup, code, report_type)

    def return_company_eq_offer_lists(self, code, company_name, reg_date=None):
        report_type = '유상증자'
        final_result = []
        start_date = dt.strftime(reg_date, '%Y%m%d') if reg_date else None
        currentPage = 1
        while True: 
            try:
                result = self.return_report_list_links(
                    code,
                    currentPage,
                    report_type,
                    company_name,
                    start_date=start_date,
                    textCrpCik=None,
                    return_final=False
                )
                if not result:
                    break 
                final_result.extend(result)
            except Exception:
                break 
            currentPage += 1
        final_result = [x for x in final_result if '기재정정' in x['title']]
        return final_result

    def return_company_report_list(self, code, company_name, reg_date=None):
        report_type_list = [YEARLY_REPORT, QUARTER_REPORT, SEMINUAL_REPORT]
        final_result = []
        for report_type in report_type_list:
            start_date = dt.strftime(reg_date, '%Y%m%d') if reg_date else None
            currentPage = 1
            while True: 
                try:
                    result = self.return_report_list_links(
                        code,
                        currentPage,
                        report_type,
                        company_name,
                        start_date=start_date,
                        textCrpCik=None
                    )
                    if not result:
                        break 
                    final_result.extend(result)
                except Exception:
                    break 
                currentPage += 1
        return final_result  
    
    def return_multiple_link_results(self, link_list, jump=30):
        success_list = []
        failed_list = []
        for i in range(0, len(link_list), jump):
            end = i + jump
            try:
                result = self.return_async_func_results(
                    'return_reportlink_data', 
                    link_list[i:end], 
                    use_callback=False
                )
            except Exception as e:
                logger.error("Error while making multiple requests " + str(e))

            for idx, r in enumerate(result):
                if isinstance(r, Exception):
                    logger.error('following result is exception ' + str(r))
                    continue
                try:
                    s, f = r
                    success_list.extend(s)
                    failed_list.extend(f)
                except Exception as e:
                    logger.error("Invalid results " + str(e))
                    print(result[idx])
        return {'success_list': success_list, 'failed_list': failed_list}

    async def parse_excel_download_link(self, soup):
        download_soup = soup.find(title="다운로드")
        if not download_soup:
            raise exception_utils.NoDataError

        download_string = download_soup.parent.get('onclick')
        search_download_link = re.search(
            "\('(\d+)'\,.*'(\d+)'\)",
            download_string
        )
        if not search_download_link:
            raise exception_utils.NoDataError
        rcpNo = search_download_link.group(1)
        downloadNo = search_download_link.group(2)
        download_link = (
            f"http://dart.fss.or.kr/pdf/download/main.do?"
            f"rcp_no={rcpNo}&dcm_no={downloadNo}"
        )
        
        download_soup = await self.return_async_get_soup(download_link)
        statements_tag = download_soup.find('td', text=re.compile("재무제표"))
        if not statements_tag:
            raise exception_utils.NoDataError
        title_text = statements_tag.text
        if '.pdf' in title_text:
            raise exception_utils.NoDataError
        report_link = statements_tag.parent.find('a').get('href')
        total_link = 'http://dart.fss.or.kr' + report_link 
        return total_link 

    async def return_excel_table_data(
        self,
        soup: BeautifulSoup,
        report_link: str,
        code: str, 
        reg_date: str, 
        corp_name: str, 
        market_type: str, 
        title: str, 
        period_type: str, 
        reporter: str, 
    ):
        try:
            link = await self.parse_excel_download_link(soup)
        except exception_utils.NoDataError:
            error_message = "No data on excel " + report_link
            raise ValueError(error_message)
        random_file_name = ''.join(
            random.choice(string.ascii_uppercase + string.digits) 
            for _ in range(10)
        ) + '.xls'
        connector = ProxyConnector(remote_resolve=False)
        async with ClientSession(
            request_class=ProxyClientRequest,
            connector=connector
        ) as session:
            async with session.get(
                link, 
                headers=self.headers, 
                allow_redirects=True,
                proxy=self.sockproxy,
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Request Error on {report_link} {link}")
                    raise exception_utils.ExpectedError
                with open('./datacollections/' + random_file_name, 'wb') as f:
                    try:
                        content = await resp.read()
                        f.write(content)
                    except Exception as e:
                        error_message = (
                            "Error saving error on "
                            f"{report_link} {link} {str(e)}"
                        )
                        raise ValueError(error_message)
        try:
            dep = DartExcelParser(
                link=report_link, 
                excel_link=link,
                fname='./datacollections/' + random_file_name,
                code=code, 
                corp_name=corp_name, 
                period_type=period_type,
                reg_date=reg_date,
                market_type=market_type, 
                reporter=reporter,
                title=title
            )
            result = dep.parse_excel_file_data()
            try:
                os.remove('./datacollections/' + random_file_name)
            except Exception:
                pass
            return result
        except Exception as e:
            error_message = (
                f"Error while parsing excel {str(e)} {report_link} {link}"
            )
            logger.error(error_message)
            try:
                os.remove('./datacollections/' + random_file_name)
            except Exception:
                pass
            raise ValueError(error_message)

    def return_update_dict(self, default_dict, update_result):
        base = copy.deepcopy(default_dict)
        base.update(update_result)
        return base
        
    async def return_reportlink_data(
        self, 
        link="",
        code="",
        reg_date=None,
        corp_name="",
        market_type="",
        title="",
        period_type=YEARLY_REPORT,
        reporter=""
    ):
        total_result = []
        failed_result = []
        default_dict = {
            'report_link': link,
            'code': code,
            'corp_name': corp_name, 
            'period_type': period_type,
            'reg_date': reg_date,
            'market_type': market_type
        }
        try:
            soup = await self.return_async_get_soup(link)
            if not soup:
                failed_info = self.return_update_dict(default_dict, {})
                failed_result.append(failed_info)
                logger.error(f"Error occured while no soup {link}")
                return [], failed_result
        except ValueError as ve:
            logger.error(f"Error occured while return soup {link} {str(ve)}")
            failed_info = self.return_update_dict(default_dict, {})
            failed_result.append(failed_info)
            return [], failed_result

        try:
            result = await self.return_excel_table_data(
                soup,
                report_link=link,
                code=code,
                reg_date=reg_date,
                corp_name=corp_name,
                market_type=market_type,
                title=title,
                period_type=period_type,
                reporter=reporter
            )
            return result, failed_result
        except ValueError as ve:
            logger.error(str(ve) + " " + link)
        except Exception as e:
            logger.error(
                f"Unexpected Error parsing excel data {str(e)} {link}"
            )

        try:
            link_data = self.parse_financial_section_link(soup)
        except exception_utils.ReportLinkParseError:
            logger.error(
                f"Error occured while return financial section {link}"
            )
            failed_info = self.return_update_dict(default_dict, {})
            failed_result.append(failed_info)
            return total_result, failed_result

        normal_table_link = link_data.get('link_fs')
        connected_table_link = link_data.get('link_connected_fs')
        # 연결재무재표가 일반재무재표와 같은페이지에있는경우 이를확인시킨다.
        is_connected_inside_normal = False
        if not connected_table_link:
            connected_table_link = normal_table_link
            is_connected_inside_normal = True

        get_table_tasks = []
        if normal_table_link:
            get_table_tasks.append(
                (
                    NORMAL_FINANCIAL_STATEMENTS,
                    normal_table_link,
                    self.return_async_get_soup(normal_table_link)
                )
            )  
        if connected_table_link:
            get_table_tasks.append(
                (
                    CONNECTED_FINANCIAL_STATEMENTS,
                    connected_table_link,
                    self.return_async_get_soup(connected_table_link)
                )
            )
        # Excel parsing 이후 table 결과값이 존재하지 않으면 결과 반환
        if not get_table_tasks:
            if normal_table_link:
                failed_info = self.return_update_dict(
                    default_dict, 
                    {
                        'table_link': normal_table_link, 
                        'report_type': NORMAL_FINANCIAL_STATEMENTS
                    }
                )
                failed_result.append(failed_info)
            if connected_table_link:
                failed_info = self.return_update_dict(
                    default_dict, 
                    {
                        'table_link': connected_table_link, 
                        'report_type': CONNECTED_FINANCIAL_STATEMENTS
                    }
                )
                failed_result.append(failed_info)

            return total_result, failed_result   

        soup_list = await asyncio.gather(
            *[x[2] for x in get_table_tasks], return_exceptions=True
        )
        for idx, soup in enumerate(soup_list):
            if isinstance(soup, Exception):
                report_type = get_table_tasks[idx][0]
                table_link = get_table_tasks[idx][1]
                failed_dict = {
                    'report_link': link,
                    'table_link': table_link,
                    'report_type': report_type
                }
                failed_info = self.return_update_dict(
                    default_dict,
                    failed_dict
                )
                failed_result.append(failed_info)
                continue

            try:
                report_type = get_table_tasks[idx][0]
                table_link = get_table_tasks[idx][1]
                drh = DartReportHandler(
                    link=link,
                    table_link=table_link,
                    soup=soup,
                    code=code, 
                    corp_name=corp_name,
                    report_type=report_type,
                    period_type=period_type,
                    reg_date=reg_date, 
                    market_type=market_type
                )
                if idx == 1 and is_connected_inside_normal:
                    result = drh.parse_report_link(
                        is_connected_inside_normal=True
                    )
                else:
                    result = drh.parse_report_link(
                        is_connected_inside_normal=False
                    )
                total_result.append(result)
            except exception_utils.ExpectedError:
                failed_dict = {
                    'report_link': link,
                    'table_link': table_link,
                    'report_type': report_type
                }
                failed_info = self.return_update_dict(
                    default_dict,
                    failed_dict
                )
                failed_result.append(failed_info)
            except Exception as e:
                logger.error(
                    f"Unexpected error occured while parsing table "
                    + str(e)
                    + table_link
                )
                failed_dict = {
                    'report_link': link, 
                    'table_link': table_link,
                    'report_type': report_type
                }
                failed_info = self.return_update_dict(
                    default_dict, failed_dict
                )
                failed_result.append(failed_info)

        # check only connected table conditions
        if len(total_result) == 2:
            final_result = []
            if (
                total_result[0]['book_value'] == total_result[1]['book_value']
                and 
                total_result[0]['sales'] == total_result[1]['sales']
            ):
                for result in total_result:
                    if result['report_type'] == CONNECTED_FINANCIAL_STATEMENTS:
                        final_result.append(result)
        else:
            final_result = total_result

        return final_result, failed_result

    def return_company_report_link_list(
        self, code, company_name, report_type, start_date=None
    ) -> List:
        final_results = []
        currentPage = 1
        while True: 
            try:
                if currentPage > 1000:
                    break
                result = self.parse_linklist_data(
                    code,
                    currentPage,
                    report_type,
                    company_name,
                    start_date=start_date,
                    textCrpCik=None
                )
                currentPage += 1
                if not result:
                    break 
                final_results.extend(result)

            except Exception:
                break 

        return final_results

    def return_company_report_data_list(
            self, 
            code: str, 
            company: str, 
            start_date=None
    ):
        try:
            report_list = self.return_company_report_link_list(
                code,
                company, 
                fp_types.YEARLY_REPORT, 
                start_date=start_date
            )
        except Exception as e: 
            errorMessage = (
                "Error occured while return report link list "
                f"{fp_types.YEARLY_REPORT} {code}, {company} "+str(e)
            )
            logger.error(errorMessage)
            raise ValueError(errorMessage)
        try:
            quarter_list = self.return_company_report_link_list(
                code,
                company, 
                fp_types.QUARTER_REPORT,
                start_date=start_date
            )
            report_list.extend(quarter_list)
        except Exception as e: 
            errorMessage = (
                "Error occured while return report link list "
                f"{fp_types.QUARTER_REPORT} {code},{company} "
                + str(e)
            )
            logger.error(errorMessage)
            raise ValueError(errorMessage)
        try:
            seminual_list = self.return_company_report_link_list(
                code,
                company, 
                fp_types.SEMINUAL_REPORT,
                start_date=start_date
            )
            report_list.extend(seminual_list)
        except Exception as e:
            errorMessage = (
                "Error occured while return report link list "
                f"{fp_types.SEMINUAL_REPORT} {code},{company} "
                + str(e)
            )
            logger.error(errorMessage)
            raise ValueError(errorMessage)
        print(len(report_list), 'report link')
        data_list = self.return_multiple_link_results(report_list)
        return data_list

    def parse_financial_section_link(self, soup: BeautifulSoup):
        head_lines = soup.find('head').text.split("\n")
        hangul = re.compile('[^ ㄱ-ㅣ가-힣]+')
        
        link_connected_fs = None
        link_fs = None
        for idx, head_line in enumerate(head_lines):
            hangul = " ".join(re.split('[^ ㄱ-ㅣ가-힣]+', head_line))
            hangul = (
                hangul
                .replace(" ", '')
                .replace('등', '')
                .replace('\n', '')
                .replace(' ', '')
                .strip()
            )
            if '재무제표' == hangul and not link_fs:
                link_fs = get_financial_statesments_links(
                    soup,
                    head_lines,
                    idx
                )
            if '연결재무제표' == hangul and not link_connected_fs:
                link_connected_fs = get_financial_statesments_links(
                    soup,
                    head_lines,
                    idx
                )
        if not link_connected_fs and not link_fs:
            raise exception_utils.ReportLinkParseError
        else:
            data = {}
            data['link_connected_fs'] = link_connected_fs 
            data['link_fs'] = link_fs 
            return data