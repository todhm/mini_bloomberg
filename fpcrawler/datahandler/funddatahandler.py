import logging
import glob
import os
import time
from typing import List, Dict
import pandas as pd
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from utils.driver_utils import return_download_path_driver


logger = logging.getLogger(__name__)


def return_fund_data_list(
    data_list: List[Dict], 
    download_path: str = '/usr/src/app/datacollections'
) -> List[Dict]:

    try:
        driver = return_download_path_driver(download_path=download_path)
    except Exception as e:
        error_message = "Error while making download path driver" + str(e)
        logger.error(error_message)
        raise ValueError(error_message)

    final_result_list = []
    for data in data_list:
        try:
            sdh = SingleReportHandler(driver, **data,download_path=download_path)
            result_list = sdh.return_fund_data()
            final_result_list.append(result_list)
        except Exception as e: 
            error_message = f"Error while parsing data {str(e)}"
            logger.error(error_message)
            raise ValueError(error_message)
    return final_result_list


class SingleReportHandler(object):
    first_level_map = {
        '국내투자형': 'radioBtn_input_0',
        '해외투자형': 'radioBtn_input_1',
        'MMF': 'radioBtn_input_2'
    }

    def __init__(
        self,
        driver,
        month=1,
        year=2010,
        first_level='해외투자형',
        second_level='채권형',
        third_level='채권투자형-일반',
        download_path='/usr/src/app/datacollections',
        **kwargs
    ):
        self.driver = driver
        self.month = month
        self.year = year
        self.first_level = first_level
        self.second_level = second_level
        self.third_level = third_level
        self.download_path = download_path
            
    def click_year_month(self):
        month = ''.zfill(2-len(str(self.month))) + str(self.month)
        year = str(self.year)
        WebDriverWait(
            self.driver, 
            10, 
            ignored_exceptions=[StaleElementReferenceException]
        ).until(
            EC.visibility_of_element_located((By.ID, 'uYear_input_0'))
        )
        year_elem = self.driver.find_element_by_id('uYear_input_0')
        WebDriverWait(
            self.driver, 
            10, 
            ignored_exceptions=[StaleElementReferenceException]
        ).until(
            EC.element_to_be_clickable(
                (By.XPATH, f'//select[@id="uYear_input_0"]/option[contains(.,"{year}")]')
            )
        )
        year_elem.find_element_by_xpath(
            f'.//option[contains(.,"{year}")]'
        ).click()
        WebDriverWait(
            self.driver, 
            10, 
            ignored_exceptions=[StaleElementReferenceException]
        ).until(
            EC.visibility_of_element_located((By.ID, 'uMonth_input_0'))
        )
        month_elem = self.driver.find_element_by_id('uMonth_input_0')    
        WebDriverWait(
            self.driver, 
            10, 
            ignored_exceptions=[StaleElementReferenceException]
        ).until(
            EC.element_to_be_clickable(
                (By.XPATH, f'//select[@id="uMonth_input_0"]/option[contains(.,"{month}")]')
            )
        )
        month_elem \
            .find_element_by_xpath(f'.//option[contains(.,"{month}")]') \
            .click()

    def select_report_data(self):
        selector_id = self.first_level_map[self.first_level]
        WebDriverWait(
            self.driver, 
            10, 
            ignored_exceptions=[StaleElementReferenceException]
        ).until(
            EC.element_to_be_clickable((By.ID, selector_id))
        )
        elem_selector = self.driver.find_element_by_id(selector_id)
        elem_selector.click()

        WebDriverWait(
            self.driver, 
            10, 
            ignored_exceptions=[StaleElementReferenceException]
        ).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH, 
                    f'//ul[@id="tblList5"]/li/a[text()="{self.second_level}"]'
                )
            )
        )

        second_elem = self.driver.find_element_by_xpath(
            f'//ul[@id="tblList5"]/li/a[text()="{self.second_level}"]'
        )
        second_elem.click()
        WebDriverWait(
            self.driver, 10, 
            ignored_exceptions=[StaleElementReferenceException]
        ).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH, 
                    f'//ul[@id="tblList6"]/li/a[text()="{self.third_level}"]'
                )
            )
        )
        thid_elem = self.driver.find_element_by_xpath(
            f'//ul[@id="tblList6"]/li/a[text()="{self.third_level}"]'
        )
        thid_elem.click()

    def download_excel_data(self, max_timeout=10):
        original_download_list = glob.glob(self.download_path + '/*.xls')
        self.driver.execute_script('sendResult();')
        WebDriverWait(
            self.driver, 15
        ).until(EC.visibility_of_element_located((By.ID, "loadingBox")))
        WebDriverWait(
            self.driver, 
            120
        ).until(EC.invisibility_of_element_located((By.ID, "loadingBox")))
        download_Btn = self.driver.find_element_by_id("excelDownBtn")
        download_Btn.click()
        time.sleep(0.4)
        while max_timeout > 0:
            list_of_files = glob.glob(self.download_path + '/*.xls')
            if len(list_of_files) > len(original_download_list):
                time.sleep(2.7)
                return max(list_of_files, key=os.path.getctime)
            max_timeout -= 3
            time.sleep(3)
        raise ValueError("Timt out for donwloading excel data")

    def convert_empty_string(self, x, type_data):
        if x == '-' and type_data is float:
            return 0.0
        elif x == '-' and type_data is int: 
            return 0 
        elif x == '-' and type_data is None: 
            return None
        return x

    def parse_excel_data(self, latest_file): 
        df = pd.read_excel(latest_file, header=[0, 1, 2])
        df.columns = [
            convert_col_list(' '.join(col).strip())
            for col in df.columns.values
        ]
        df['stock_amount'] = df['stock_amount'].apply(
            lambda x: self.convert_empty_string(x, int)
        )
        df['stock_ratio'] = df['stock_ratio'].apply(
            lambda x: self.convert_empty_string(x, float)
        )
        df['bond_amount'] = df['bond_amount'].apply(
            lambda x: self.convert_empty_string(x, int)
        )
        df['bond_ratio'] = df['bond_ratio'].apply(
            lambda x: self.convert_empty_string(x, float)
        )
        df['security_contract_amount'] = df['security_contract_amount'].apply(
            lambda x: self.convert_empty_string(x, int)
        )
        df['security_contract_ratio'] = df['security_contract_ratio'].apply(
            lambda x: self.convert_empty_string(x, float)
        )
        df['real_estate_amount'] = df['real_estate_amount'].apply(
            lambda x: self.convert_empty_string(x, int)
        )
        df['real_estate_ratio'] = df['real_estate_ratio'].apply(
            lambda x: self.convert_empty_string(x, float)
        )
        df['goods_amount'] = df['goods_amount'].apply(
            lambda x: self.convert_empty_string(x, int)
        )
        df['goods_ratio'] = df['goods_ratio'].apply(
            lambda x: self.convert_empty_string(x, float)
        )
        df['cash_amount'] = df['cash_amount'].apply(
            lambda x: self.convert_empty_string(x, int)
        )
        
        df['one_month_rank'] = df['one_month_rank'].apply(
            lambda x: self.convert_empty_string(x, None)
        )
        df['six_month_rank'] = df['six_month_rank'].apply(
            lambda x: self.convert_empty_string(x, None)
        )
        
        df['one_year_rank'] = df['one_year_rank'].apply(
            lambda x: self.convert_empty_string(x, None)
        )
        df['end_period_rank'] = df['end_period_rank'].apply(
            lambda x: self.convert_empty_string(x, None)
        )
        df['year'] = self.year
        df['month'] = self.month 
        df['first_level'] = self.first_level
        df['second_level'] = self.second_level
        df['third_level'] = self.third_level
        return df.to_dict('records')

    def return_fund_data(self):
        try:
            self.driver.set_page_load_timeout(10)
            self.driver.get("http://dis.kofia.or.kr/websquare/index.jsp?w2xPath=/wq/fundann/DISFundROPCmpAnn.xml&divisionId=MDIS01009001000000&serviceId=SDIS01009001000")
            WebDriverWait(
                self.driver, 
                20, 
                ignored_exceptions=[StaleElementReferenceException]
            ).until(
                EC.invisibility_of_element_located((By.ID, "loadingBox"))
            )
        except Exception as e:
            raise ValueError("Error cannot load first page" + str(e))

        try:
            WebDriverWait(
                self.driver, 
                20, 
                ignored_exceptions=[StaleElementReferenceException]
            ).until(
                EC.visibility_of_element_located((By.ID, "uYear_input_0"))
            )
            self.click_year_month()
        except Exception as e: 
            raise ValueError("Cannot click date info " + str(e))

        try:
            self.select_report_data()
        except Exception as e: 
            raise ValueError("Cannot select report data " + str(e))

        try:
            excel_path = self.download_excel_data()
        except Exception as e: 
            raise ValueError("Error while donwloading excel data " + str(e))
            
        try:
            data_list = self.parse_excel_data(excel_path)
        except Exception as e: 
            raise ValueError("Error parsing excel data " + str(e))
        try:
            os.remove(excel_path)
        except Exception: 
            pass
        return data_list
          

def convert_col_list(col):
    if '운용회사' in col:
        return 'company'
    elif '펀드명' in col:
        return 'name'
    elif '위험등급' in col:
        return 'riskrate'
    elif '구분\n(추가/\n단위)' in col:
        return 'class'
    elif '기준\n통화' in col:
        return 'currency'
    elif '순자산\n총액\n(원화:억원\n외화:백만)' in col:
        return 'total_amount'
    elif '보유자산 금액' in col and '주식 금액' in col:
        return 'stock_amount'
    elif '보유자산 금액' in col and '주식 비중' in col:
        return 'stock_ratio'
    elif '보유자산 금액' in col and '채권 금액' in col:
        return 'bond_amount'
    elif '보유자산 금액' in col and '채권 비중' in col:
        return 'bond_ratio'
    elif '보유자산 금액' in col and '투자계약증권 금액' in col:
        return 'security_contract_amount'
    elif '보유자산 금액' in col and '투자계약증권 비중' in col:
        return 'security_contract_ratio'
    elif '보유자산 금액' in col and '부동산 비중' in col:
        return 'real_estate_ratio'
    elif '보유자산 금액' in col and '부동산 금액' in col:
        return 'real_estate_amount'
    elif '보유자산 금액' in col and '실물등 금액' in col:
        return 'goods_amount'
    elif '보유자산 금액' in col and '실물등 비중' in col:
        return 'goods_ratio'
    elif '보유자산 금액' in col and '현금성 금액' in col:
        return 'cash_amount'
    elif '보유자산 금액' in col and '현금성 비중' in col:
        return 'cash_ratio'
    elif '보유자산 금액' in col and '기타 금액' in col:
        return 'etc_amount'
    elif '보유자산 금액' in col and '기타 비중' in col:
        return 'etc_ratio'
    elif '기준\n가격' in col:
        return 'standard_price'
    elif '수익률 및 순위 1개월 %' in col:
        return 'one_month_return'
    elif '수익률 및 순위 1개월 순위' in col:
        return 'one_month_rank'
    elif '수익률 및 순위 1개월 순위' in col:
        return 'one_month_rank'
    elif '수익률 및 순위 6개월 %' in col:
        return 'six_month_return'
    elif '수익률 및 순위 6개월 순위' in col:
        return 'six_month_rank'
    elif '수익률 및 순위 1년 %' in col:
        return 'one_year_return'
    elif '수익률 및 순위 1년 순위' in col:
        return 'one_year_rank'
    elif '수익률 및 순위 설정(전환)일 %' in col:
        return 'end_period_return'
    elif '수익률 및 순위 설정(전환)일 순위' in col:
        return 'end_period_rank'
    elif '운용' in col and '인력' in col:
        return 'members'
    elif '보수(%)' in col and '운용' in col:
        return 'operational_cost'
    elif '보수(%)' in col and '판매 보수' in col:
        return 'sales_cost'
    elif '보수(%)' in col and '판매 수수료' in col:
        return 'sales_comission'
    elif '보수(%)' in col and '판매 수수료' in col:
        return 'sales_comission'
    elif '환헤지' in col:
        return 'currency_hedge'
    elif '주요' in col and '투자' in col and '지역' in col:
        return 'invest_region'
    elif '판매회사' in col:
        return 'sales_company'