from datetime import datetime as dt ,timedelta
import requests
import pymongo 
import re 
from pymongo import MongoClient 
from bs4 import BeautifulSoup
from scrapy.http import Response, Request, HtmlResponse
from fp_types import petovski_cash_dict
import lxml.html as LH
from lxml.etree import tostring as htmlstring
from utils import DataHandlerClass,return_async_get_soup,return_sync_get_soup,ReturnSoupError,return_proxy_list
from utils.exception_utils import ReportLinkParseError,NotableError
from fp_types import YEARLY_REPORT,QUARTER_REPORT,CONNECTED_FINANCIAL_STATEMENTS,NORMAL_FINANCIAL_STATEMENTS
import logging
import re 


logger = logging.getLogger()


def find_value(text, unit):
    print(text,type(text))
    if type(text) is str:
        final_text = text.replace(" ","").replace("△","-").replace("(-)","-").replace("(","-").replace(")","").replace(",","").replace("=","").replace("-",'').replace('원','').replace('주','').replace('Δ','').replace("-",'').replace("_","")
    else:
        final_text = ""
    if not final_text:
        return 0

    try:
        return float(final_text)*unit
    except ValueError:
        if len(re.findall('\.',final_text))>=2:
            return float(final_text.replace('.','')) * unit 
        return 0
        print('Find value error',text)
        raise ValueError
    


class DartDataHandler(DataHandlerClass):



    def __init__(self,db_name='testdb',*args,**kwargs):
        super().__init__(db_name)
        self.korean_company_list = self.db.korean_company_list
        self.company_report_list = self.db.company_report_list
        self.company_data_list = self.db.company_data_list
        self.db_name = db_name
        self.headers=  {'Cookie':'DSAB002_MAXRESULTS=5000;'}
        self.proxy_list = return_proxy_list()
        self.proxy_idx = -1
        self.proxy_object = None 
        

    
        




    def parse_single_page_data(self,code,currentPage,report_type,company_name,start_date=None,textCrpCik=None):
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
            data = requests.post(url,data=tpdata)
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
            data_object['reg_date'] = dt.strptime(date,"%Y-%m-%d")
            data_object['corp_name'] = corp_name
            data_object['market_type'] = market
            data_object['title'] = title
            data_object['period_type'] = report_type
            data_object['reporter'] = reporter
            data_object_list.append(data_object)
        if data_object_list:
            self.company_report_list.insert_many(data_object_list)
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
            data_object['reg_date'] = dt.strptime(date,"%Y-%m-%d")
            data_object['corp_name'] = corp_name
            data_object['market_type'] = market
            data_object['title'] = title
            data_object['period_type'] = report_type
            data_object['reporter'] = reporter
            data_object_list.append(data_object)
        return data_object_list



    def return_report_list_links(self,code,currentPage,report_type,company_name,start_date=None,textCrpCik=None,return_final=True):
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
            data = requests.post(url,data=tpdata)
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
        return self.parse_dart_search_result(soup,code,report_type)

        
    def return_company_eq_offer_lists(self,code,company_name,reg_date=None):
        report_type = '유상증자'
        final_result = []
        start_date = dt.strftime(reg_date,'%Y%m%d') if reg_date else None
        currentPage = 1
        while True: 
            try:
                result = self.return_report_list_links(code,currentPage,report_type,company_name,start_date=start_date,textCrpCik=None,return_final=False)
                result = [  i for i in result if '기재정정' not in i.get('title')]

                if not result:
                    break 
                final_result.extend(result)
            except Exception as e:
                print(e)
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
                    print(e)
                    break 

                currentPage += 1
        return final_result


    def parse_units(self,unit_table):
        unit_regex_xpath = ".//p[re:match(text(), '단[\s]*위')]/text()|.//td[re:match(text(), '단[\s]*위')]/text()"                    
        unit_regex=re.compile("(\d+(,\d+)*)원")
        p_tag_list = unit_table.xpath(unit_regex_xpath)
        unit_ptag_text = p_tag_list[0].extract()
        if unit_regex.search(unit_ptag_text):
            unit = unit_regex.search(unit_ptag_text).group(1)
            unit = float(unit.replace(',',''))
        else:
            unit_regex=re.compile("\s*\S*원")
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

            


    def parse_finance_table(self,re_data,scrapyResponse):
        first_style_balance_table_list = scrapyResponse.xpath(f"//table//p[re:match(text(), '{re_data}')]/ancestor::table/following-sibling::table | //table//p[re:match(text(), '{re_data}')]/ancestor::table")
        if len(first_style_balance_table_list)>=2:
            unit = self.parse_units(first_style_balance_table_list[0])
            balancesheet_table = first_style_balance_table_list[1].extract()
            return balancesheet_table,unit
            
        else:
            first_style_balance_table_list = scrapyResponse.xpath(f"//p[re:match(text(), '{re_data}')]/following-sibling::table")
            if len(first_style_balance_table_list) >=2:
                unit_table = first_style_balance_table_list[0]
                unit = self.parse_units(unit_table)
                balancesheet_table = first_style_balance_table_list[1].extract()
                return balancesheet_table,unit
            else:
                raise ValueError


    def return_financial_report_table(self,link):
        return_data = {}
        response = requests.get(link)
        request = Request(url=link)
        scrapyResponse = HtmlResponse(
            url=link, request=request, body=response.content)
        balance_re = "재[ \s]*무[ \s]*상[ \s]*태[ \s]*표|대[ \s]*차[ \s]*대[ \s]*조[ \s]*표|재[ \s]*무[ \s]*상[ \s]*태[ \s]*표"
        secondary_balance_re ="유[ \s]*동[ \s]*자[ \s]*산"
        income_re = "포[ \s]*괄[ \s]*손[ \s]*익[ \s]*계[ \s]*산[ \s]*서|손[ \s]*익[ \s]*계[ \s]*산[ \s]*서"
        secondary_income_re = ""
        cashflow_re = "현[ \s]*금[ \s]*흐[ \s]*름[ \s]*표"
        secondary_cashflow_re=""
        all_table_list = scrapyResponse.xpath("//table")
        if len(all_table_list) <2:
            raise NotableError
        unit_regex_xpath = f".//p[re:match(text(), '단[\s]*위')]/text()"
        unit_regex=re.compile("(\d+(,\d+)*)원")
        try:
            balancesheet_table,unit = self.parse_finance_table(balance_re,scrapyResponse)
            return_data['balance_sheet'] = {}
            return_data['balance_sheet']['table'] = balancesheet_table
            return_data['balance_sheet']['unit'] = unit
        except ValueError:
            logger.error("No balancesheet data" + link)
        try:
            income_table,unit = self.parse_finance_table(income_re,scrapyResponse)
            return_data['income_statement'] = {}
            return_data['income_statement']['table'] = income_table
            return_data['income_statement']['unit'] = unit

        except ValueError:
            logger.error("No income statement data" + link)

        try:
            cashflow_table,unit = self.parse_finance_table(cashflow_re,scrapyResponse)
            return_data['cashflow'] ={}
            return_data['cashflow']['table'] = cashflow_table
            return_data['cashflow']['unit'] = unit

        except ValueError:
            logger.error("No cashflow data" + link)  
        return return_data

    def parse_cashflow_table(self,table,unit):
        root = LH.fromstring(table)

        parsed_petovski_data = {}
        for key in petovski_cash_dict:
            result = root.xpath(f"//tr//*[re:match(text(),'{key}')]/ancestor::tr", namespaces={'re': 'http://exslt.org/regular-expressions'})
            if result:
                td_results = result[0].xpath('.//td')
                number_string = td_results[1].text_content().encode().decode()
                value = find_value(number_string,unit)
                parsed_petovski_data[petovski_cash_dict[key]] = value

        

        




            


            



    def save_company_report_list(self,code,company_name):
        report_type_list = [YEARLY_REPORT,QUARTER_REPORT]
        for report_type in report_type_list:
            report_data = list(self.company_report_list.find({"code":code,'period_type':report_type},sort=[('reg_date',pymongo.DESCENDING)]).limit(1))
            if report_data:
                start_date = report_data[0]['reg_date']
                start_date = start_date + timedelta(days=1)
                start_date = dt.strftime(start_date,'%Y%m%d')
            else:
                start_date = None
            currentPage = 1
            while True: 
                try:
                    result = self.parse_single_page_data(code,currentPage,report_type,company_name,start_date=start_date,textCrpCik=None)
                    if not result:
                        break 
                except Exception as e:
                    print(e)
                    break 

                currentPage += 1



    def get_financial_statesments_links(self,soup2,head_lines,line_find):


        if len(head_lines) -1 < line_find+4:
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



    def parse_multiple_columns_table(self,data_range_length,tds,unit,cashflow_data_object,list_data_name,data_key,first_is_note):
        value = 0
        for k in range(len(data_range_length[0])):
            try:
                value = find_value(tds[data_range_length[0][k]].text.strip(), unit)
                if value and value>0:
                    break
            except:
                pass
        
        cashflow_data_object[data_key] = value
        for temp_idx,temp_list in  enumerate(data_range_length):
            for j in range(len(data_range_length[temp_idx])):
                try:
                    period_value = find_value(tds[data_range_length[temp_idx][j]].text.strip(), unit)
                except:
                    period_value = None
                    pass
            try:
                if period_value:
                    cashflow_data_object[list_data_name][temp_idx][data_key] =period_value
            except:
                pass
        return cashflow_data_object 
    
    
        
    # Get information of balance sheet
    def parse_balancesheet_info(self,balance_sheet_table, year, unit,period_list):
        re_asset_list = []

        re_asset_current				=	re.compile("^유[ \s]*동[ \s]*자[ \s]*산([ \s]*합[ \s]*계)*|\.[ \s]*유[ \s]*동[ \s]*자[ \s]*산([ \s]*합[ \s]*계)*")
        re_asset_current_sub1			=	re.compile("현[ \s]*금[ \s]*및[ \s]*현[ \s]*금[ \s]*((성[ \s]*자[ \s]*산)|(등[ \s]*가[ \s]*물))")
        re_asset_current_sub2			=	re.compile("매[ \s]*출[ \s]*채[ \s]*권")
        re_asset_current_sub3			=	re.compile("재[ \s]*고[ \s]*자[ \s]*산")
        re_asset_non_current			=	re.compile("비[ \s]*유[ \s]*동[ \s]*자[ \s]*산|고[ \s]*정[ \s]*자[ \s]*산([ \s]*합[ \s]*계)*")
        re_asset_non_current_sub1		=	re.compile("유[ \s]*형[ \s]*자[ \s]*산")
        re_asset_non_current_sub2		=	re.compile("무[ \s]*형[ \s]*자[ \s]*산")
        re_asset_sum					=	re.compile("자[ \s]*산[ \s]*총[ \s]*계([ \s]*합[ \s]*계)*|자[ \s]*산[ \s]*총[ \s]*계")
        re_liability_current			=	re.compile("^유[ \s]*동[ \s]*부[ \s]*채([ \s]*합[ \s]*계)*|\.[ \s]*유[ \s]*동[ \s]*부[ \s]*채([ \s]*합[ \s]*계)*")
        re_liability_current_sub1		=	re.compile("매[ \s]*입[ \s]*채[ \s]*무[ \s]*")
        re_liability_current_sub2		=	re.compile("단[ \s]*기[ \s]*차[ \s]*입[ \s]*금")
        re_liability_current_sub3		=	re.compile("미[ \s]*지[ \s]*급[ \s]*금[ \s]*")
        re_liability_current_sub4		=	re.compile("미[ \s]*지[ \s]*급[ \s]*비[ \s]*용")
        re_liability_current_sub5		=	re.compile("단[ \s]*기[ \s]*차[ \s]*입[ \s]*금")
        re_liability_current_sub6		=	re.compile("선[ \s]*수[ \s]*금")

        re_liability_non_current		=	re.compile("^비[ \s]*유[ \s]*동[ \s]*부[ \s]*채|\.[ \s]*비[ \s]*유[ \s]*동[ \s]*부[ \s]*채|고[ \s]*정[ \s]*부[ \s]*채")
        re_liability_non_current_sub1	=	re.compile("사[ \s]*채[ \s]*")
        re_long_term_debt           	=	re.compile("장[ \s]*기[ \s]*차[ \s]*입[ \s]*금")
        re_liability_non_current_sub3	=	re.compile("장[ \s]*기[ \s]*미[ \s]*지[ \s]*급[ \s]*금")
        re_liability_non_current_sub4	=	re.compile("이[ \s]*연[ \s]*법[ \s]*인[ \s]*세[ \s]*부[ \s]*채")
        re_liability_sum				=	re.compile("^부[ \s]*채[ \s]*총[ \s]*계([ \s]*합[ \s]*계)*|\.[ \s]*부[ \s]*채[ \s]*총[ \s]*계([ \s]*합[ \s]*계)*")
        re_equity						=	re.compile("자[ \s]*본[ \s]*금")
        re_equity_sub1					=	re.compile("주[ \s]*식[ \s]*발[ \s]*행[ \s]*초[ \s]*과[ \s]*금")
        re_equity_sub3					=	re.compile("자[ \s]*본[ \s]*잉[ \s]*여[ \s]*금")
        re_equity_sub2					=	re.compile("이[ \s]*익[ \s]*잉[ \s]*여[ \s]*금")
        re_equity_sum					=	re.compile("^자[ \s]*본[ \s]*총[ \s]*계([ \s]*합[ \s]*계)*|\.[ \s]*자[ \s]*본[ \s]*총[ \s]*계([ \s]*합[ \s]*계)*")
        re_common_stock_equity			=	re.compile("보[ \s]*통[ \s]*주[ \s]*자[ \s]*본[ \s]*금")

        re_asset_list.append(re_asset_current)
        re_asset_list.append(re_asset_current_sub1)
        re_asset_list.append(re_asset_current_sub2)		
        re_asset_list.append(re_asset_current_sub3)		
        re_asset_list.append(re_asset_non_current)
        re_asset_list.append(re_asset_non_current_sub1)	
        re_asset_list.append(re_asset_non_current_sub2)	
        re_asset_list.append(re_asset_sum)
        re_asset_list.append(re_liability_current)
        re_asset_list.append(re_liability_current_sub1)
        re_asset_list.append(re_liability_current_sub2)		
        re_asset_list.append(re_liability_current_sub3)		
        re_asset_list.append(re_liability_current_sub4)		
        re_asset_list.append(re_liability_current_sub5)		
        re_asset_list.append(re_liability_current_sub6)		

        re_asset_list.append(re_liability_non_current)
        re_asset_list.append(re_liability_non_current_sub1)	
        re_asset_list.append(re_long_term_debt)	
        re_asset_list.append(re_liability_non_current_sub3)	
        re_asset_list.append(re_liability_non_current_sub4)	
        re_asset_list.append(re_liability_sum)
        re_asset_list.append(re_equity)
        re_asset_list.append(re_equity_sub1)
        re_asset_list.append(re_equity_sub3)
        re_asset_list.append(re_equity_sub2)		
        re_asset_list.append(re_equity_sum)
        re_asset_list.append(re_common_stock_equity)

        balance_sheet_sub_list = {}
        
        balance_sheet_key_list = []

        balance_sheet_key_list.append("asset_current")
        balance_sheet_key_list.append("asset_current_sub1")
        balance_sheet_key_list.append("asset_current_sub2")
        balance_sheet_key_list.append("asset_current_sub3")
        balance_sheet_key_list.append("asset_non_current")
        balance_sheet_key_list.append("asset_non_current_sub1")
        balance_sheet_key_list.append("asset_non_current_sub2")
        balance_sheet_key_list.append("asset_sum")
        balance_sheet_key_list.append("liability_current")			
        balance_sheet_key_list.append("liability_current_sub1")		
        balance_sheet_key_list.append("liability_current_sub2")		
        balance_sheet_key_list.append("liability_current_sub3")		
        balance_sheet_key_list.append("liability_current_sub4")		
        balance_sheet_key_list.append("liability_current_sub5")		
        balance_sheet_key_list.append("liability_current_sub6")		

        balance_sheet_key_list.append("liability_non_current")		
        balance_sheet_key_list.append("liability_non_current_sub1")	
        balance_sheet_key_list.append("long_term_debt")	
        balance_sheet_key_list.append("liability_non_current_sub3")	
        balance_sheet_key_list.append("liability_non_current_sub4")	
        balance_sheet_key_list.append("liability_sum")				
        balance_sheet_key_list.append("equity")						
        balance_sheet_key_list.append("equity_sub1")				
        balance_sheet_key_list.append("equity_sub3")				
        balance_sheet_key_list.append("equity_sub2")				
        balance_sheet_key_list.append("equity_sum")	
        balance_sheet_key_list.append("common_stock_equity")	

				
        balance_sheet_sub_list = self.parse_table_info(balance_sheet_table,unit,period_list,re_asset_list,balance_sheet_key_list,[],table_type="balance")
        return balance_sheet_sub_list


    def parse_table_info(self,table,unit,period_list,re_cashflow_list,cashflow_key_list,error_cashflows_list,table_type="cash"):


        if table_type == 'cash':
            list_data_name = 'cash_period_data_list'
            error_key_name = "cashflow_error"
            key_start = "cashFlowPeriodStart"
            key_end = 'cashFlowPeriodEnd'
            period_key = "cashflow_date"
        elif table_type == 'income':
            list_data_name = 'income_period_data_list'
            error_key_name = "income_error"
            key_start = "incomePeriodStart"
            key_end = 'incomePeriodEnd'
            period_key = "income_date"
        else:
            list_data_name = 'balancesheet_period_data_list'
            error_key_name = "balancesheet_error"
            key_start = "balancesheetPeriodStart"
            key_end = 'balancesheetPeriodEnd'
            period_key = "balancesheet_date"


            
        cashflow_data_object  = {}
        for key in cashflow_key_list:
            cashflow_data_object[key] = 0


        #net_income = 0.0

        #첫번쨰 행이 주석인지 처리 
        trs = table.findAll("tr")
        if not trs:
            raise ValueError

        tr = trs[0]
        first_td = tr.findAll('td')
        if len(first_td) ==0 :
            first_td = tr.findAll('th')
        data_length = len(first_td) -1
        
        cashflow_data_object[list_data_name] = []
        not_data_list_idx = []
        first_is_note = False
        subject_bars = []
        re_subject = re.compile('과[ \s]*목')

        for idx,td in enumerate(first_td):
            re_note = re.compile('주[ \s]*석|과[ \s]*목|구[ \s]*분|^[^\w]+$')
            if re_note.search(td.text) or td.text.replace(' ','').replace('\t','').replace('\n','') =='':
                not_data_list_idx.append(idx)
            if re_subject.search(td.text):
                subject_bars.append(idx)
        for period_idx in range(len(not_data_list_idx),len(first_td)):
            cashflow_data_object[list_data_name].append({})

        if len(first_td)>1:
            second_head = first_td[1]
            re_note = re.compile('주[ \s]*석|과[ \s]*목')
            if re_note.search(second_head.text):
                first_is_note = True 
                data_length -= 1

        # 해당 리포트에 담아있는 모든 period의 정보를 담을 수 있도록 준비
        if len(period_list)>0:
            period_data = period_list[0]
            if len(period_data)>=1:
                start_time = dt.strptime(period_data[0],'%Y.%m.%d')
                if len(period_data)>1:
                    end_time = dt.strptime(period_data[1],'%Y.%m.%d')
                    cashflow_data_object[key_start] = start_time
                    cashflow_data_object[key_end] = end_time
                    cashflow_data_object[period_key] = end_time
                else:
                    cashflow_data_object[period_key] = start_time     

                                                    
        if period_list :
            for idx,idx2 in zip(range(len(cashflow_data_object[list_data_name])),range(len(period_list))):
                period_data = period_list[idx]
                start_time = dt.strptime(period_data[0],'%Y.%m.%d')
                if len(period_data) >1:
                    end_time = dt.strptime(period_data[1],'%Y.%m.%d')
                    cashflow_data_object[list_data_name][idx][key_start] = start_time
                    cashflow_data_object[list_data_name][idx][key_end] = end_time
                    cashflow_data_object[list_data_name][idx][period_key] = end_time

                else:
                    cashflow_data_object[list_data_name][idx][period_key] = start_time

        # CASHFLOW statement
        if (len(trs) != 2):

            for tr in trs:
                tds = tr.findAll("td")
                try:
                    if len(tds) >=2:
                        
                        value = None
                        for i in range(len(re_cashflow_list)):  

                            re_search_word = re_cashflow_list[i]
                            data_key = cashflow_key_list[i]
                            added_length = len(not_data_list_idx)
                            tds_data_length = len(tds) - len(not_data_list_idx)

                            if re_search_word.search(tds[0].text.strip()):
                                if len(cashflow_data_object[list_data_name]) ==tds_data_length:


                                    data_idx_list = set(range(len(tds))) - set(not_data_list_idx)
                                    data_idx_list = list(data_idx_list)
                                    if not data_idx_list:
                                        raise ValueError 

                                    first_idx = data_idx_list[0]
                                    value = find_value(tds[first_idx].text.strip(), unit)
                                    cashflow_data_object[data_key] = value if value else 0 
                                    if len(subject_bars) <2:
                                        for period_idx in range(len(cashflow_data_object[list_data_name])):
                                            td_idx = data_idx_list[period_idx]
                                            period_value = find_value(tds[td_idx].text.strip(), unit)
                                            cashflow_data_object[list_data_name][period_idx][cashflow_key_list[i]] =period_value
                                    else:
                                        for period_idx in range(len(cashflow_data_object[list_data_name])):
                                            for j,re_sub in enumerate(re_cashflow_list):
                                                if re_sub.search(tds[subject_bars[period_idx]].text.strip()):
                                                    td_idx = data_idx_list[period_idx]
                                                    period_value = find_value(tds[td_idx].text.strip(), unit)
                                                    cashflow_data_object[list_data_name][period_idx][cashflow_key_list[j]] =period_value

                                #데이터가 테이블의 좌우로 나누어져서 등록되어있을 때
                                elif tds_data_length==len(cashflow_data_object[list_data_name])*2:
                                    data_range_length = [ [i*2+added_length,i*2+1+added_length] for i in range(len(cashflow_data_object[list_data_name]))]
                                    cashflow_data_object = self.parse_multiple_columns_table(data_range_length,tds,unit,cashflow_data_object,list_data_name,data_key,first_is_note)

                                else:

                                    if len(cashflow_data_object[list_data_name])>0:
                                        difference_length = tds_data_length- len(cashflow_data_object[list_data_name])
                                        data_container_length = tds_data_length//len(cashflow_data_object[list_data_name])
                                        data_range_length = []
                                        if data_container_length>0:
                                            for i  in range(0,tds_data_length,data_container_length):
                                                length_range = list(range(i+added_length,i+data_container_length+added_length))
                                                data_range_length.append(length_range)
                                            cashflow_data_object = self.parse_multiple_columns_table(data_range_length,tds,unit,cashflow_data_object,list_data_name,data_key,first_is_note)
                                            
                    elif len(tds)>=1:
                        error_cashflows_list.append(tds[0].text.strip())
                except Exception as e:
                    print("NET INCOME PARSING ERROR in Cashflows line 353")
                    print(error_key_name,i)
                    cashflow_data_object[error_key_name] = "PARSING ERROR"
                    print(e)
                    raise ValueError
        # Special case
        ## if (len(trs) != 2):
        else:	
            tr = trs[1]
            tds = tr.findAll("td")

            index_col = []
            prev = 0
            for a in tds[0].childGenerator():
                if (str(a) == "<br/>"):
                    if (prev == 1):
                        index_col.append('')	
                    prev = 1
                else:
                    prev = 0
                    index_col.append(str(a).strip())	
            data_col = []
            prev = 0
            for b in tds[1].childGenerator():
                if (str(b) == "<br/>"):
                    if (prev == 1):
                        data_col.append('0')	
                    prev = 1
                else:
                    data_col.append(str(b))	
                    prev = 0
            data_col2 = []
            prev = 0
            for b in tds[2].childGenerator():
                if (str(b) == "<br/>"):
                    if (prev == 1):
                        data_col2.append('')	
                    prev = 1
                else:
                    data_col2.append(str(b))	
                    prev = 0


            index_cnt = 0

            for (index) in (index_col):
                try:
                    value = 0.0
                    for i in range(len(re_cashflow_list)):
                        re_search_word = re_cashflow_list[i]

                        if re_search_word.search(index):
                            if len(tds)>4:
                                if (data_col[index_cnt].strip() != '') and (data_col[index_cnt].strip() != '-'):
                                    value = find_value(data_col[index_cnt], unit)
                                    break
                                elif (data_col2[index_cnt].strip() != '') and (data_col2[index_cnt].strip() != '-'):
                                    value = find_value(data_col2[index_cnt], unit)
                                    break
                            else:
                                if (data_col[index_cnt].strip() != '') and (data_col[index_cnt].strip() != '-'):
                                    value = find_value(data_col[index_cnt], unit)
                                    break
                    if value != 0.0 and cashflow_data_object[cashflow_key_list[i]] != None:
                        cashflow_data_object[cashflow_key_list[i]] = value
                except Exception as e:
                    print("PARSING ERROR")
                    print('PARSING error in second lines')
                    print(e)
                    raise ValueError

                    cashflow_data_object[error_key_name] = "PARSING ERROR"
                index_cnt = index_cnt + 1
        return cashflow_data_object




    def parse_cashflow_info(self,cashflow_table, unit,period_list):
        error_cashflows_list = []
        re_cashflow_list = []

        # Regular expression
        re_op_cashflow			= re.compile("((영업활동)|(영업활동으로[ \s]*인한)|(영업활동으로부터의))[ \s]*([순]*현금[ \s]*흐름)")
        re_op_cashflow_sub1 	= re.compile("((영업에서)|(영업으로부터))[ \s]*창출된[ \s]*현금(흐름)*")
        re_op_cashflow_sub2 	= re.compile("(연[ \s]*결[ \s]*)*당[ \s]*기[ \s]*순[ \s]*((이[ \s]*익)|(손[ \s]*익))")
        re_op_cashflow_sub3 	= re.compile("감[ \s]*가[ \s]*상[ \s]*각[ \s]*비")
        re_op_cashflow_sub4 	= re.compile("신[ \s]*탁[ \s]*계[ \s]*정[ \s]*대")
        re_op_none_cash_income    = re.compile('현[ \s]*금[ \s]*의[ \s]*유[ \s]*입[ \s]*이[ \s]*없[ \s]*는[ \s]*수[ \s]*익')
        re_op_cash_add_cost     = re.compile('현[ \s]*금[ \s]*의[ \s]*유[ \s]*출[ \s]*이[ \s]*없[ \s]*는[ \s]*비[ \s]*용[ \s]*등[ \s]*의[ \s]*가[ \s]*산')
        re_op_asset_debt_change = re.compile('영업활동으로부터의[ \s]*자[ \s]*산[ \s]*부[ \s]*채[ \s]*|영업활동으로 인한 자[ \s]*산[ \s]*부[ \s]*채')

        re_invest_cashflow		= re.compile("투자[ \s]*활동[ \s]*현금[ \s]*흐름|투[ \s]*자[ \s]*활[ \s]*동[ \s]*으[ \s]*로[ \s]*인[ \s]*한[ \s]*[순]*현[ \s]*금[ \s]*흐[ \s]*름")
        re_invest_cashflow_sub1 = re.compile("유[ \s]*형[ \s]*자[ \s]*산[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub2 = re.compile("무[ \s]*형[ \s]*자[ \s]*산[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub3 = re.compile("토[ \s]*지[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub4 = re.compile("건[ \s]*물[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub5 = re.compile("구[ \s]*축[ \s]*물[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub6 = re.compile("기[ \s]*계[ \s]*장[ \s]*치[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub7 = re.compile("건[ \s]*설[ \s]*중[ \s]*인[ \s]*자[ \s]*산[ \s]*의[ \s]*((증[ \s]*가)|(취[ \s]*득))")
        re_invest_cashflow_sub8 = re.compile("차[ \s]*량[ \s]*운[ \s]*반[ \s]*구[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub9 = re.compile("비[ \s]*품[ \s]*의[ \s]*취[ \s]*득|비[ \s]*품[ \s]*의[ \s]*((증[ \s]*가)|(취[ \s]*득))")
        re_invest_cashflow_sub10= re.compile("공[ \s]*구[ \s]*기[ \s]*구[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub11= re.compile("시[ \s]*험[ \s]*연[ \s]*구[ \s]*설[ \s]*비[ \s]*의[ \s]*취[ \s]*득")
        re_invest_cashflow_sub12= re.compile("렌[ \s]*탈[ \s]*자[ \s]*산[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub13= re.compile("영[ \s]*업[ \s]*권[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub14= re.compile("산[ \s]*업[ \s]*재[ \s]*산[ \s]*권[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub15= re.compile("소[ \s]*프[ \s]*트[ \s]*웨[ \s]*어[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub16= re.compile("기[ \s]*타[ \s]*무[ \s]*형[ \s]*자[ \s]*산[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub17= re.compile("투[ \s]*자[ \s]*부[ \s]*통[ \s]*산[ \s]*의[ \s]*((취[ \s]*득)|(증[ \s]*가))")
        re_invest_cashflow_sub18= re.compile("관[ \s]*계[ \s]*기[ \s]*업[ \s]*투[ \s]*자[ \s]*의[ \s]*취[ \s]*득|관계[ \s]*기업[ \s]*투자[ \s]*주식의[ \s]*취득|지분법[ \s]*적용[ \s]*투자[ \s]*주식의[ \s]*취득")

        re_fin_cashflow			= re.compile("재무[ \s]*활동[ \s]*현금[ \s]*흐름|재무활동으로[ \s]*인한[ \s]*현금흐름")
        re_fin_cashflow_sub1	= re.compile("단기차입금의[ \s]*순증가")
        re_fin_cashflow_sub2	= re.compile("배당금[ \s]*지급|현금배당금의[ \s]*지급|배당금의[ \s]*지급|현금배당|보통주[ ]*배당[ ]*지급")
        re_fin_cashflow_sub3	= re.compile("자기주식의[ \s]*취득")
        re_start_cash			= re.compile("기초[ ]*현금[ ]*및[ ]*현금성[ ]*자산|기초의[ \s]*현금[ ]*및[ ]*현금성[ ]*자산|기[ \s]*초[ \s]*의[ \s]*현[ \s]*금|기[ \s]*초[ \s]*현[ \s]*금")
        re_end_cash				= re.compile("기말[ ]*현금[ ]*및[ ]*현금성[ ]*자산|기말의[ \s]*현금[ ]*및[ ]*현금성[ ]*자산|기[ \s]*말[ \s]*의[ \s]*현[ \s]*금|기[ \s]*말[ \s]*현[ \s]*금")

        re_cashflow_list.append(re_op_cashflow)
        re_cashflow_list.append(re_op_cashflow_sub1) 	
        re_cashflow_list.append(re_op_cashflow_sub2) 	
        re_cashflow_list.append(re_op_cashflow_sub3) 	
        re_cashflow_list.append(re_op_cashflow_sub4)
        re_cashflow_list.append(re_op_none_cash_income) 	
        re_cashflow_list.append(re_op_cash_add_cost)
        re_cashflow_list.append(re_op_asset_debt_change)

        re_cashflow_list.append(re_invest_cashflow)		
        re_cashflow_list.append(re_invest_cashflow_sub1) 
        re_cashflow_list.append(re_invest_cashflow_sub2) 
        re_cashflow_list.append(re_invest_cashflow_sub3) 
        re_cashflow_list.append(re_invest_cashflow_sub4) 
        re_cashflow_list.append(re_invest_cashflow_sub5) 
        re_cashflow_list.append(re_invest_cashflow_sub6) 
        re_cashflow_list.append(re_invest_cashflow_sub7) 
        re_cashflow_list.append(re_invest_cashflow_sub8) 
        re_cashflow_list.append(re_invest_cashflow_sub9) 
        re_cashflow_list.append(re_invest_cashflow_sub10)
        re_cashflow_list.append(re_invest_cashflow_sub11)
        re_cashflow_list.append(re_invest_cashflow_sub12)
        re_cashflow_list.append(re_invest_cashflow_sub13)
        re_cashflow_list.append(re_invest_cashflow_sub14)
        re_cashflow_list.append(re_invest_cashflow_sub15)
        re_cashflow_list.append(re_invest_cashflow_sub16)
        re_cashflow_list.append(re_invest_cashflow_sub17)
        re_cashflow_list.append(re_invest_cashflow_sub18)

        re_cashflow_list.append(re_fin_cashflow)		
        re_cashflow_list.append(re_fin_cashflow_sub1)	
        re_cashflow_list.append(re_fin_cashflow_sub2)	
        re_cashflow_list.append(re_fin_cashflow_sub3)	
        re_cashflow_list.append(re_start_cash)
        re_cashflow_list.append(re_end_cash)


        # 영업현금흐름
        ## 영업에서 창출된 현금흐름
        ## 당기순이익
        ## 신탁계정대
        # 투자현금흐름
        ## 유형자산의 취득
        ## 무형자산의 취득
        ## 토지의 취득
        ## 건물의 취득
        ## 구축물의 취득
        ## 기계장치의 취득
        ## 건설중인자산의증가
        ## 차량운반구의 취득
        ## 영업권의 취득
        ## 산업재산권의 취득
        ## 기타의무형자산의취득
        ## 투자부동산의 취득
        ## 관계기업투자의취득
        # 재무현금흐름
        ## 단기차입금의 순증가
        ## 배당금 지급
        ## 자기주식의 취득
        # 기초 현금 및 현금성자산
        # 기말 현금 및 현금성자산


        cashflow_key_list = []

        cashflow_key_list.append("op_cashflow")
        cashflow_key_list.append("op_cashflow_sub1")
        cashflow_key_list.append("op_cashflow_sub2")
        cashflow_key_list.append("op_cashflow_sub3")
        cashflow_key_list.append("op_cashflow_sub4")
        cashflow_key_list.append("op_none_cash_income") 	

        cashflow_key_list.append('op_cash_add_cost')
        cashflow_key_list.append('op_asset_debt_change')

        cashflow_key_list.append("invest_cashflow")
        cashflow_key_list.append("invest_cashflow_sub1")
        cashflow_key_list.append("invest_cashflow_sub2")
        cashflow_key_list.append("invest_cashflow_sub3")
        cashflow_key_list.append("invest_cashflow_sub4")
        cashflow_key_list.append("invest_cashflow_sub5")
        cashflow_key_list.append("invest_cashflow_sub6")
        cashflow_key_list.append("invest_cashflow_sub7")
        cashflow_key_list.append("invest_cashflow_sub8")
        cashflow_key_list.append("invest_cashflow_sub9")
        cashflow_key_list.append("invest_cashflow_sub10")
        cashflow_key_list.append("invest_cashflow_sub11")
        cashflow_key_list.append("invest_cashflow_sub12")
        cashflow_key_list.append("invest_cashflow_sub13")
        cashflow_key_list.append("invest_cashflow_sub14")
        cashflow_key_list.append("invest_cashflow_sub15")
        cashflow_key_list.append("invest_cashflow_sub16")
        cashflow_key_list.append("invest_cashflow_sub17")
        cashflow_key_list.append("invest_cashflow_sub18")
        cashflow_key_list.append("fin_cashflow")
        cashflow_key_list.append("fin_cashflow_sub1")
        cashflow_key_list.append("fin_cashflow_sub2")
        cashflow_key_list.append("fin_cashflow_sub3")
        cashflow_key_list.append("start_cash")
        cashflow_key_list.append("end_cash")
        cashflow_data_object = self.parse_table_info(cashflow_table,unit,period_list,re_cashflow_list,cashflow_key_list,error_cashflows_list,table_type="cash")
        return cashflow_data_object


    def scrape_income_statement(self,income_table, year, unit, mode,period_list):

        #매출액
        #매출원가
        #매출총이익
        #판매비와관리비
        #영업이익
        #기타수익
        #기타비용
        #금융수익
        #금융비용
        #법인세비용차감전순이익
        #번인세비용
        #당기순이익
        #기본주당이익
        re_income_list = []

        # Regular expression
        re_sales			=	re.compile("^매[ \s]*출[ \s]*액|\.[ \s]*매[ \s]*출[ \s]*액|\(매출액\)")
        re_sales_sub1		= 	re.compile("^매[ \s]*출[ \s]*원[ \s]*가|\.[ \s]*매[ \s]*출[ \s]*원[ \s]*가")
        re_gross_margin		= 	re.compile("^매[ \s]*출[ \s]*총[ \s]*이[ \s]*익|\.[ \s]*매[ \s]*출[ \s]*총[ \s]*이[ \s]*익")
        re_gross_loss		= 	re.compile("^매[ \s]*출[ \s]*총[ \s]*손[ \s]*실|\.[ \s]*매[ \s]*출[ \s]*총[ \s]*손[ \s]*실")
        re_sales_sub3		= 	re.compile("판[ \s]*매[ \s]*비[ \s]*와[ \s]*관[ \s]*리[ \s]*비")
        re_sales_finance	= 	re.compile("^금[ \s]*융[ \s]*수[ \s]*익")
        re_income_finance	= 	re.compile("순[ \s]*금[ \s]*융[ \s]*수[ \s]*익")


        re_sales2			=	re.compile("^영[ \s]*업[ \s]*수[ \s]*익|\.[ \s]*영[ \s]*업[ \s]*수[ \s]*익")
        re_sales2_sub1		= 	re.compile("^영[ \s]*업[ \s]*비[ \s]*용|\.[ \s]*영[ \s]*업[ \s]*비[ \s]*용")
        re_op_income		= 	re.compile("^영[ \s]*업[ \s]*이[ \s]*익|\.[ \s]*영[ \s]*업[ \s]*이[ \s]*익")
        re_op_income_sub1	= 	re.compile("기[ \s]*타[ \s]*수[ \s]*익")
        re_op_income_sub2	= 	re.compile("기[ \s]*타[ \s]*비[ \s]*용")
        re_op_income_sub3	= 	re.compile("금[ \s]*융[ \s]*수[ \s]*익")
        re_op_income_sub4	= 	re.compile("금[ \s]*융[ \s]*비[ \s]*용")
        re_op_income_sub6	= 	re.compile("영[ \s]*업[ \s]*외[ \s]*수[ \s]*익")
        re_op_income_sub7	= 	re.compile("영[ \s]*업[ \s]*외[ \s]*비[ \s]*용")
        re_op_income_sub5	= 	re.compile("법[ \s]*인[ \s]*세[ \s]*비[ \s]*용[ \s]*차[ \s]*감[ \s]*전[ \s]*순[ \s]*((이[ \s]*익)|(손[ \s]*실))|법[ \s]*인[ \s]*세[ \s]*차[ \s]*감[ \s]*전[ \s]*계[ \s]*속[ \s]*영[ \s]*업[ \s]*순[ \s]*이[ \s]*익|법인세[ \s]*차감전[ \s]*순이익|법인세차감전계속영업이익|법인세비용차감전이익|법인세비용차감전계속영업[순]*이익|법인세비용차감전당기순이익|법인세비용차감전순이익|법인세비용차감전[ \s]*계속사업이익|법인세비용차감전순손익")
        re_other_sales	    = 	re.compile("영[ \s]*업[ \s]*외[ \s]*수[ \s]*익")
        re_other_costs	    = 	re.compile("영[ \s]*업[ \s]*외[ \s]*비[ \s]*용")

        re_tax				=	re.compile("법[ \s]*인[ \s]*세[ \s]*비[ \s]*용")
        re_net_income =	re.compile("^순[ \s]*이[ \s]*익|^당[ \s]*기[ \s]*순[ \s]*이[ \s]*익|^연[ ]*결[ ]*[총 ]*당[ ]*기[ ]*순[ ]*이[ ]*익|지배기업의 소유주에게 귀속되는 당기순이익|[ \s]*분[ \s]*기[ \s]*순[ \s]*이[ \s]*익|당\(분\)기순이익|\.[ \s]*당[ \s]*기[ \s]*순[ \s]*이[ \s]*익|당분기연결순이익|[ \s]*당[ \s]*기[ \s]*순[ \s]*이[ \s]*익")
        re_net_loss =	re.compile("^순[ \s]*손[ \s]*실|당[ \s]*기[ \s]*순[ \s]*손[ \s]*실|[ \s]*분[ \s]*기[ \s]*순[ \s]*손[ \s]*실|당\(분\)기순손실|당분기연결순손실")

        re_eps				=	re.compile("기[ \s]*본[ \s]*주[ \s]*당[ \s]*((수[ \s]*익)|([순 \s]*이[ \s]*익))")
        re_special_income		= 	re.compile("특[ \s]*별[ \s]*이[ \s]*익")
        re_special_loss		= 	re.compile("특[ \s]*별[ \s]*손[ \s]*실")


        re_income_list.append(re_sales)	
        re_income_list.append(re_sales_sub1)		 	
        re_income_list.append(re_gross_margin)		 	
        re_income_list.append(re_gross_loss)		 	
        re_income_list.append(re_sales_sub3)	
        re_income_list.append(re_sales_finance)	
        re_income_list.append(re_income_finance)	
	 	
        re_income_list.append(re_sales2)	
        re_income_list.append(re_sales2_sub1)		 	
        re_income_list.append(re_op_income)		 	
        re_income_list.append(re_op_income_sub1)	 	
        re_income_list.append(re_op_income_sub2)	 	
        re_income_list.append(re_op_income_sub3)	 	
        re_income_list.append(re_op_income_sub4)	 	
        re_income_list.append(re_op_income_sub5)	 	
        re_income_list.append(re_op_income_sub6)	 	
        re_income_list.append(re_op_income_sub7)	 	
        re_income_list.append(re_other_sales)	 	
        re_income_list.append(re_other_costs)	 	

        re_income_list.append(re_tax)
        re_income_list.append(re_net_income)
        re_income_list.append(re_net_loss)

        re_income_list.append(re_eps)	
        re_income_list.append(re_special_income)			
        re_income_list.append(re_special_loss)			

      

        income_statement_key_list = []
        income_statement_key_list.append("sales")			
        income_statement_key_list.append("sales_sub1")		
        income_statement_key_list.append("gross_margin")		
        income_statement_key_list.append("gross_loss")		
        income_statement_key_list.append("sales_sub3")	
        income_statement_key_list.append("sales_finance")		
        income_statement_key_list.append("income_finance")		

        income_statement_key_list.append("sales2")			
        income_statement_key_list.append("sales2_sub1")		
        income_statement_key_list.append("op_income")		
        income_statement_key_list.append("op_income_sub1")	
        income_statement_key_list.append("op_income_sub2")	
        income_statement_key_list.append("op_income_sub3")	
        income_statement_key_list.append("op_income_sub4")	
        income_statement_key_list.append("op_income_sub5")	
        income_statement_key_list.append("op_income_sub6")	
        income_statement_key_list.append("op_income_sub7")	
        income_statement_key_list.append("other_sales")	
        income_statement_key_list.append("other_costs")	
        income_statement_key_list.append("tax")			
        income_statement_key_list.append("net_income")		
        income_statement_key_list.append("net_loss")		
        income_statement_key_list.append("eps")			
        income_statement_key_list.append("special_income")			
        income_statement_key_list.append("special_loss")			

   
        income_statement_sub_list = self.parse_table_info(income_table,unit,period_list,re_income_list,income_statement_key_list,[],table_type="income")
        return income_statement_sub_list


    def parse_date_info(self,tables,table_num):
        pass


    def parse_unit_string(self,tables,table_num):
        unit = 100.0
        unit_find = 0
        re_unit1 = re.compile('단위[ \s]*:[ \s]*원')
        re_unit2 = re.compile('단위[ \s]*:[ \s]*백만원')
        re_unit3 = re.compile('단위[ \s]*:[ \s]*천원')

        # 원
        if len(tables[table_num-1](string=re_unit1)) != 0:
            unit = 1
            unit_find = 1
        # 백만원
        elif len(tables[table_num-1](string=re_unit2)) != 0:
            unit = 1000000
            unit_find = 1
        elif len(tables[table_num-1](string=re_unit3)) != 0:
            unit = 1000
            unit_find = 1
        return unit

    def parse_period_from_table(self,period_table):
        p_list = period_table.findAll('p')
        re_date = re.compile('\d+\s*\.\s*\d+\s*\.\s*\d+|\d+\s*년\s*\d+\s*월\s*\d+일')
        re_date_group = re.compile('(\d+)\D*(\d+)\D*(\d+)')

        period_list= []
        for p in p_list:
            date_list = re_date.findall(p.text)
            if date_list:
                for date_idx,date in enumerate(date_list):
                        group_data = re_date_group.match(date)
                        year = group_data.group(1)
                        month = group_data.group(2)
                        day = group_data.group(3)
                        month = month if int(month) <= 12 else 12
                        date_list[date_idx] = f'{year}.{month}.{day}'
                    
                #date_list = [ p.replace(' ','').replace('년','.').replace('월','.').replace('일','').replace(u'\xa','').replace(u'\xa0','') for p in  date_list if p ]
                #기수가 3개 이상일 경우에는 ~부터 ~까지일 가능성이 없으므로 하나의 list에 하나의 column이 있다고 가정
                if len(date_list)>2:
                    for date in date_list:
                        period_list.append([date])

                else:
                    period_list.append(date_list)

        if not period_list:
            p_list = period_table.findAll('td')
            for p in p_list:
                date_list = re_date.findall(p.text)
                if date_list:

                    for date_idx,date in enumerate(date_list):
                        group_data = re_date_group.match(date)
                        year = group_data.group(1)
                        month = group_data.group(2)
                        day = group_data.group(3)
                        month = month if int(month) <= 12 else 12

                        date_list[date_idx] = f'{year}.{month}.{day}'
                        
                    if len(date_list)>2:
                        for date in date_list:
                            period_list.append([date])
                    else:
                        period_list.append(date_list)
        return period_list

    def parse_period_list(self,tables,idx):
        period_list = []
        if idx > 0:
            period_table = tables[idx-1]
            period_list = self.parse_period_from_table(period_table)
            period_list = period_list if len(period_list)<=3 else []

        if not period_list:
            if idx > 1:
                period_table = tables[idx-2]
                period_list = self.parse_period_from_table(period_table)
                period_list = period_list if len(period_list)<=3 else []

        for x in period_list:
            try:
                temp_time = dt.strptime("%Y.%m.%d",x)
            except:
                period_list = []
                break

        

        return period_list
       

    def parse_financial_statements_table(self,tables,p_list):
        re_income_find = re.compile("법[ \s]*인[ \s]*세[ \s]*비[ \s]*용(\(이익\))*[ \s]*차[ \s]*감[ \s]*전[ \s]*순[ \s]*((이[ \s]*익)|(손[ \s]*실))|법[ \s]*인[ \s]*세[ \s]*차[ \s]*감[ \s]*전[ \s]*계[ \s]*속[ \s]*영[ \s]*업[ \s]*순[ \s]*이[ \s]*익|법인세[ \s]*차감전[ \s]*순이익|법인세차감전계속영업이익|법인세비용차감전이익|법인세비용차감전계속영업[순]*이익|법인세비용차감전당기순이익|법인세(비용차감|손익가감)전순이익|법인세비용차감전[ \s]*계속사업이익|법인세비용차감전순손익|[ \s]*당[ \s]*기[ \s]*순[ \s]*이[ \s]*익[ \s]*|포[ \s]*괄[ \s]*손[ \s]*익[ \s]*계[ \s]*산[ \s]*서")
        re_cashflow_find = re.compile("영업활동[ \s]*현금[ \s]*흐름|영업활동으로[ \s]*인한[ \s]*[순]*현금[ \s]*흐름|영업활동으로부터의[ \s]*현금흐름|영업활동으로 인한 자산부채의 변동|현[ \s]*금[ \s]*흐[ \s]*름[ \s]*표")
        re_balance_sheet_find = re.compile("현[ \s]*금[ \s]*및[ \s]*현[ \s]*금[ \s]*((성[ \s]*자[ \s]*산)|(등[ \s]*가[ \s]*물))|재[ \s]*무[ \s]*상[ \s]*태[ \s]*표|대[ \s]*차[ \s]*대[ \s]*조[ \s]*표|재[ \s]*무[ \s]*상[ \s]*태[ \s]*표")
        table_num = 0
        cash_table_find = False 
        income_table_find = False 
        balance_table_find = False 

        balance_sheet_sub_list = {}
        cashflow_sub_list = {}
        income_statement_sub_list = {}


        target_idx_list = []
        for idx,table in enumerate(tables):
            if all([cash_table_find,income_table_find,balance_table_find]):
                break

            p_data_list = table.find_previous_sibling('p',attrs={'class':'table-group'})

            if ((re_balance_sheet_find.search(table.text)) or (p_data_list  and re_balance_sheet_find.search(p_data_list.text))) and not balance_table_find:
                target_idx = idx
                re_asset_current = re.compile("^유[ \s]*동[ \s]*자[ \s]*산([ \s]*합[ \s]*계)*|\.[ \s]*유[ \s]*동[ \s]*자[ \s]*산([ \s]*합[ \s]*계)*|유[ \s]*동[ \s]*자[ \s]*산[ \s]*|자[ \s]*산[ \s]*총[ \s]*계[ \s]*|자[ \s]*산[ \s]*합[ \s]*계[ \s]*")
                if not table.find('thead') and not  re_asset_current.search(table.text):
                    if len(tables) > idx+1:
                        table = tables[idx+1]    
                        target_idx = idx + 1
                        if  not table.find('thead') and not  re_asset_current.search(table.text):
                            continue
                    else:
                        continue
                if target_idx in target_idx_list:
                    continue
                else:
                    target_idx_list.append(target_idx)
                unit = self.parse_unit_string(tables,target_idx)
                period_list = self.parse_period_list(tables,target_idx)                
                balance_sheet_sub_list = self.parse_balancesheet_info(table, reg_date, unit,period_list)
                balance_table_find = True

            elif ((re_income_find.search(table.text)) or (p_data_list  and re_income_find.search(p_data_list.text)) \
                or('당기순이익' in table.text and '매출액' in table.text)) and not income_table_find:
                target_idx = idx
                re_op_income = 	re.compile("^영[ \s]*업[ \s]*이[ \s]*익|[ \s]*영[ \s]*업[ \s]*이[ \s]*익")
                re_net_income =	re.compile("순[ \s]*이[ \s]*익|순[\s]*손[\s]*실")
                if not table.find('thead') and not re_net_income.search(table.text) and not re_op_income.search(table.text):
                    if len(tables) > idx+1:
                        table = tables[idx+1]    
                        target_idx = idx + 1
                        if not table.find('thead') and not re_net_income.search(table.text) and not re_op_income.search(table.text):
                            continue
                    else:
                        continue
                if target_idx in target_idx_list:
                    continue
                else:
                    target_idx_list.append(target_idx)
                unit = self.parse_unit_string(tables,target_idx)
                period_list = self.parse_period_list(tables,target_idx)
                income_statement_sub_list = self.scrape_income_statement(table, reg_date, unit, 1,period_list)
                income_table_find = True
            
            elif ((re_cashflow_find.search(table.text)) or (p_data_list  and re_cashflow_find.search(p_data_list.text))) and not cash_table_find :
                target_idx = idx
                re_op_cashflow = re.compile("((영업활동)|(영업활동으로[ \s]*인한)|(영업활동으로부터의))[ \s]*([순]*현금[ \s]*흐름)|영업활동으로[\s]*인한[\s]*현금[\s]*흐름")
                if  not table.find('thead') and  not re_op_cashflow.search(table.text) :
                    
                    if len(tables) > idx+1:
                        table = tables[idx+1]    
                        target_idx = idx + 1
                        if not table.find('thead') and not re_op_cashflow.search(table.text):
                            continue
                    else:
                        continue

                if target_idx in target_idx_list:
                    continue
                else:
                    target_idx_list.append(target_idx)
                unit = self.parse_unit_string(tables,target_idx)
                period_list = self.parse_period_list(tables,target_idx)
                cashflow_sub_list = self.parse_cashflow_info(table, unit,period_list)
                cash_table_find = True

                
        

        if sum([cash_table_find,income_table_find,balance_table_find]) >=2:
            result= {}
            result['cashflow_result'] = cashflow_sub_list
            result['income_statement_sub_list'] = income_statement_sub_list
            result['balance_sheet_sub_list'] = balance_sheet_sub_list
            return result 
        else:
            return None
    


    def save_company_all_financial_statements_data(self,code):
        title_list = list(self.company_data_list.find({"company_code":int(code)},{'title':1}))
        title_list = [ x['title'] for x in title_list if x.get('title')]
        report_list = list(self.company_report_list.find({"code":int(code),"title":{"$nin":title_list}}))
        for i in range(0,len(report_list),5):
            end = i + 5
            report_temp_list = report_list[i:end]
            self.call_async_func('save_finance_statements_info',report_temp_list,use_callback=False)

    def save_company_sync_data(self,code):
        title_list = list(self.company_data_list.find({"company_code":int(code)},{'title':1}))
        title_list = [ x['title'] for x in title_list if x.get('title')]
        report_list = list(self.company_report_list.find({"code":int(code),"title":{"$nin":title_list}}))
        for report in report_list:
            print(report.get('link'))
            self.save_finance_statements_info(**report)

    def change_proxy_object(self):
        if len(self.proxy_list) > self.proxy_idx + 1:
            self.proxy_idx += 1
            self.proxy_object = self.proxy_list[self.proxy_idx]

    def parse_financial_section_link(self,soup):
        #print(soup)
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
            if hangul.replace(" ",'').replace('등','').replace('\n','') =='재무제표':
                link_fs = self.get_financial_statesments_links(soup,head_lines,idx)
            if hangul.replace(" ",'') =='연결재무제표':
                line_find_bak = line_num
                link_connected_fs =  self.get_financial_statesments_links(soup,head_lines,idx)
        if not link_connected_fs and not link_fs:
            raise ReportLinkParseError
        else:
            data = {}
            data['link_connected_fs'] = link_connected_fs 
            data['link_fs'] = link_fs 
            return data 


    def parse_financial_tables(self,soup):
        connected_tables = soup.findAll("table")
        p_list = soup.findAll("p",attrs={'class':'table-group'})
      


    async def save_finance_statements_info(self,**kwargs):
        link = kwargs.get('link')
        reg_date = kwargs.get('reg_date')
        report_name = kwargs.get('title')
        period_type = kwargs.get('period_type')
        company_code = kwargs.get('code')
        try:
            # soup = return_sync_get_soup(link)
            soup = await return_async_get_soup(link,self.proxy_object)
        except ReturnSoupError as e:
            self.change_proxy_object()
            print('Error in whole report parsing',link,self.proxy_object)
            return False 
        if soup:
            try: 
                link_data = self.parse_financial_section_link(soup)
                link_connected_fs = link_data.get('link_connected_fs')
                link_fs = link_data.get('link_fs')
            except ReportLinkParseError: 
                print('cannot parse link parse error')
                return False 

            if link_connected_fs:
                try:
                    # soup3 = return_sync_get_soup(link_connected_fs)
                    soup3 = await return_async_get_soup(link_connected_fs,self.proxy_object)
                except ReturnSoupError as e: 
                    self.change_proxy_object()
                    print('Error in connected get network error',link_connected_fs,self.proxy_object)
                    return False 
                
                connected_tables = soup3.findAll("table")
                p_list = soup3.findAll("p",attrs={'class':'table-group'})

                if connected_tables and len(connected_tables)>=2:
                    try:
                        result = self.parse_financial_statements_table(connected_tables,reg_date,p_list)
                    except Exception as e:
                        print(company_code,report_name,link,e,"it's an error")
                        raise ValueError 
                    if result:
                        cashflow_data = result['cashflow_result']
                        income_statement_sub_list = result['income_statement_sub_list']
                        balance_sheet_sub_list = result['balance_sheet_sub_list']
                        start_data = {}
                        start_data.update(cashflow_data)
                        start_data.update(income_statement_sub_list)
                        start_data.update(balance_sheet_sub_list)
                        if not start_data.get('asset_sum') or  start_data.get('asset_sum')==0:
                            print(company_code,report_name,link,"no data from connected_table")
                        else:    
                            start_data['reg_date'] = reg_date
                            start_data['company_code'] = company_code
                            start_data['title'] = report_name
                            start_data['period_type'] = period_type
                            start_data['report_type'] = CONNECTED_FINANCIAL_STATEMENTS
                            self.company_data_list.insert_one(start_data)
                    else:
                        print(company_code,report_name,link,'connected')



            if link_fs:
                try:
                    soup3 = await return_async_get_soup(link_fs,self.proxy_object)
                except ReturnSoupError as e: 
                    self.change_proxy_object()
                    print('Error in normal report get network error',link_fs,self.proxy_object)
                    return False 
                
                normal_tables = soup3.findAll("table")
                p_list = soup3.findAll("p",attrs={'class':'table-group'})
                if normal_tables and len(normal_tables)>=2:
                    try:
                        result = self.parse_financial_statements_table(normal_tables,reg_date,p_list)
                    except Exception as e:
                        print(company_code,report_name,link,e,"it's an error")
                        raise ValueError 
                    if result:
                        cashflow_data = result['cashflow_result']
                        income_statement_sub_list = result['income_statement_sub_list']
                        balance_sheet_sub_list = result['balance_sheet_sub_list']
                        start_data = {}
                        start_data.update(cashflow_data)
                        start_data.update(income_statement_sub_list)
                        start_data.update(balance_sheet_sub_list)
                        start_data['reg_date'] = reg_date
                        start_data['company_code'] = company_code
                        start_data['title'] = report_name
                        start_data['period_type'] = period_type
                        start_data['report_type'] = NORMAL_FINANCIAL_STATEMENTS
                        self.company_data_list.insert_one(start_data)

                    else:
                        print(company_code,report_name,link,'normal')
