import unittest
from tests.basetest import BaseTest
from datetime import datetime as dt 
from datetime import timedelta 
import requests 
from scrapy.http import Response, Request, HtmlResponse

class UtilityTest(BaseTest):



    def test_utility_test(self):
        url = 'https://finance.naver.com/item/sise_day.nhn?code=018250&page=1'
        response = requests.get(url)
        request = Request(url=url)
        scrapyResponse = HtmlResponse(
            url=url, request=request, body=response.content)
        text_list = scrapyResponse.xpath('//table/tr[position()>1]')

        for text in text_list:
            table_data = text.xpath('./td/span/text()').extract()
            if not table_data:
                continue
            table_data = [ data.strip().replace(',','') for data in table_data if data ]
            print(table_data)

    
