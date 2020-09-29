import asyncio
import json
from aiohttp import ClientSession, ClientTimeout, TCPConnector


def async_post_functions(url, data_list, timeout=1000, request_counts=10):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    timeout = ClientTimeout(total=timeout)
    conn = TCPConnector(limit=request_counts)
    session = ClientSession(connector=conn, loop=loop)
    async_list = []
    for idx, item in enumerate(data_list):
        async_list.append(call_async_post(session, url, item))
    all_results = asyncio.gather(*async_list, return_exceptions=True)
    results = loop.run_until_complete(all_results)
    loop.close()
    success_list = []
    failed_list = []
    for idx, x in enumerate(results):
        if not isinstance(x, Exception):
            success_list.append(x)
        else:
            print(str(x), 'error', data_list[idx])
            failed_list.append(data_list[idx])
    return success_list, failed_list


async def call_async_post(session, url, data, retry_attempts=4):
    async with session.post(
        url, 
        json=data, 
    ) as response:
        try:
            text = await response.text()
            json_data = json.loads(text)
            if response.status != 200:
                error_message = json_data.get("errorMessage", '')
                raise ValueError("Not a valid request " + error_message)
        except Exception as e: 
            raise ValueError(str(e))
    return json_data


async def async_report_call(loop, url, data_list, timeout=1000, request_counts=5):
    timeout = ClientTimeout(total=timeout)
    conn = TCPConnector(limit=request_counts)
    async_list = []
    async with ClientSession(connector=conn, timeout=timeout) as session:
        for item in data_list:
            async_list.append(call_async_post(session, url, item))
        all_results = await asyncio.gather(*async_list, return_exceptions=True)
    return all_results
    

def report_api_call(url, data_list, timeout=1000, request_counts=5):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(
        async_report_call(
            loop, 
            url, 
            data_list, 
            timeout=timeout, 
            request_counts=request_counts
        )
    )
    loop.close()
    success_list = []
    failed_list = []
    for idx, x in enumerate(results):
        if not isinstance(x, Exception):
            print('success counts', len(x['success_list']), data_list[idx])
            print('failed counts', len(x['failed_list']), data_list[idx])
            success_list.append(x)
        else:
            print(str(x), 'error', data_list[idx])
            failed_list.append(data_list[idx])
    return success_list, failed_list