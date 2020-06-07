import asyncio
import aiohttp
import json 
from bs4 import BeautifulSoup
import requests 


user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14'
headers = {'User-Agent': user_agent,'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'}


class ReturnSoupError(Exception):
    pass


async def return_async_get(url):
    return_data = {}
    async with aiohttp.ClientSession() as s:
        async with s.get(url,headers=headers) as resp:
            try:
                result_string = await resp.text()
                result_json = json.loads(result_string)
                return_data['result'] = "success"
                return_data['data'] = result_json 
            except Exception as e:
                return_data['result'] = "fail"
                return_data['errorMessage'] = str(e)
            return return_data 
            
async def return_async_get_soup(url,proxy_object =None):
    return_data = {}
    #ip = 'https://194.126.183.141'
    try:
        async with aiohttp.ClientSession(trust_env=True) as s:
            if proxy_object:
                proxy_ip = proxy_object['ip']
                proxy_port = proxy_object['port']
                http_proxy  = f"http://{proxy_ip}:{proxy_port}"
                async with s.get(url,headers=headers,proxy=http_proxy) as resp:
                    result_string = await resp.text()
                    soup = BeautifulSoup(result_string)
                    return soup
            else:
                async with s.get(url,headers=headers) as resp:
                    result_string = await resp.text()
                    soup = BeautifulSoup(result_string,'html.parser')
                    return soup

    except Exception as e:
        raise ValueError(f"Error making async requests {url}" +str(e))


def return_sync_get_soup(url,proxy_object =None):
    try:
        if proxy_object:
            proxy_ip = proxy_object['ip']
            proxy_port = proxy_object['port']
            proxy_url = f'{proxy_ip:proxy_port}'
            proxies = {}
            proxies['http'] = proxy_url
            proxies['https'] = proxy_url
            data = requests.get(url,headers=headers,proxies=proxies).text
        else:
            data = requests.get(url,headers=headers).text
        soup = BeautifulSoup(data,'html.parser')
    except Exception as e:
        raise ReturnSoupError
    return soup


def return_proxy_list():
    proxy_list = requests.get('https://free-proxy-list.net/').text
    soup = BeautifulSoup(proxy_list)
    table= soup.find('table',attrs={'id':'proxylisttable'})
    tr_list = table.find('tbody').findAll('tr')
    td_list = [ r.findAll('td') for r in tr_list]
    ip_list = [ {'ip':x[0].text ,'port':x[1].text} for x in td_list]
    return ip_list