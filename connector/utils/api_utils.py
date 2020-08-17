import json
import asyncio
from aiohttp import ClientSession
from aiohttp_retry import RetryClient


async def call_async_post(url, data, retry_attempts=2):
    statuses = {x for x in range(100, 600)}
    statuses.remove(200)
    async with ClientSession() as client:
        retry_client = RetryClient(client)
        async with retry_client.post(
            url, 
            json=data, 
            retry_attempts=retry_attempts, 
            retry_for_statuses=statuses
        ) as response:
            text = await response.text()
        await retry_client.close()
    return json.loads(text)


def async_post_functions(url, data_list):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    async_list = []
    for idx, item in enumerate(data_list):
        async_list.append(call_async_post(url, item))
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


def report_api_call(url, data_list):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    async_list = []
    for idx, item in enumerate(data_list):
        async_list.append(call_async_post(url, item))
    all_results = asyncio.gather(*async_list, return_exceptions=True)
    results = loop.run_until_complete(all_results)
    loop.close()
    success_list = []
    failed_list = []
    for idx, x in enumerate(results):
        if not isinstance(x, Exception):
            print(len(x['success_list']), data_list[idx])
            print(len(x['failed_list']), data_list[idx])
            success_list.append(x)
        else:
            print(str(x), 'error', data_list[idx])
            failed_list.append(data_list[idx])
    return success_list, failed_list
