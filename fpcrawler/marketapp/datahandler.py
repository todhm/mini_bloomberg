import csv
from io import BytesIO, TextIOWrapper
import aiohttp


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def value_converter(k, v):
    key = key_converter(k)
    if key in ['Code', 'Name']:
        return v
    elif not v:
        return ""
    return float(v.replace(',', ''))


def key_converter(k):
    if '순위' in k:
        k = '순위'
    data_map = {
      '순위': 'Rank',
      '종목코드': 'Code',
      '종목명': 'Name',
      '현재가': 'Close',
      '대비': 'Changes',
      '등락률': 'ChangesRatio',
      '거래량': 'Volume',
      '거래대금': 'Amount',
      '시가': 'Open',
      '고가': 'High',
      '저가': 'Low',
      '시가총액': 'Marcap',
      '시가총액비중(%)': 'MarcapRatio',
      '상장주식수': 'Stocks',
      '외국인 보유주식수': 'ForeignShares',
      '외국인 지분율(%)': 'ForeignRatio',
      '총카운트': 'TotalCount'
    }
    return data_map[k]
        

async def return_report_data(datestring):
    headers = {'User-Agent': 'Chrome/78 Safari/537'}
    otpurl = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    params = {
        'name': 'fileDown',
        'filetype': 'csv',
        'url': 'MKD/04/0404/04040200/mkd04040200_01',
        'market_gubun': 'ALL',
        'indx_ind_cd': '',
        'sect_tp_cd': 'ALL',
        'schdate': datestring,
        'pagePath': '/contents/MKD/04/0404/04040200/MKD04040200.jsp',
    }
    async with aiohttp.ClientSession() as s:
        async with s.get(otpurl, params=params, headers=headers) as resp:
            try:
                code_string = await resp.text()
                params = {'code': code_string}                
            except Exception as e:
                raise ValueError("Error while making otp krx" + str(e))
                
            downloadurl = 'http://file.krx.co.kr/download.jspx'
            headers = {
                'Referer': "http://marketdata.krx.co.kr/mdi",
                'User-Agent': 'Chrome/78 Safari/537',
            }
            async with s.post(
                downloadurl, params=params, headers=headers
            ) as resp:
                try:
                    content = await resp.read()
                    with TextIOWrapper(
                        BytesIO(content), 
                        encoding='utf-8', 
                        newline=''
                    ) as text_file:
                        data_list = []
                        for row in csv.DictReader(
                            text_file, 
                            skipinitialspace=True
                        ):
                            newrow = {
                                key_converter(k): value_converter(k, v) 
                                for k, v in row.items()
                            }
                            newrow['Date'] = datestring
                            data_list.append(newrow)
                except Exception as e: 
                    raise ValueError("Error while  donwloads" + str(e))
            return data_list
