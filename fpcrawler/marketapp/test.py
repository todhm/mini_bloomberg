import pytest
from tests.test_app import client


@pytest.mark.markettest
def test_market_data_api():
    date_string_list = ['20200518', '20200504', '202005019']
    response = client.post(
        '/return_market_data', 
        json={'dateList': date_string_list}
    )
    data_list = response.json()
    assert(len(data_list) == 3)
    assert(len(data_list[0]) > 1000)
    first_ten_data = data_list[0][:10]
    for data in first_ten_data:
        assert(type(data['Code']) == str)
        assert(data['Code'].startswith('00') is True)
        assert(type(data['Name']) == str)
        assert(type(data['Close']) == float)
        assert(type(data['Changes']) == float)
        if not type(data['ChangesRatio']) is str:
            assert(type(data['ChangesRatio']) == float)
        else:
            assert(data['ChangesRatio'] == '')

        assert(type(data['Volume']) == float)
        assert(type(data['Amount']) == float)
        assert(type(data['Open'])is float)
        assert(type(data['High']) == float)
        assert(type(data['Low']) == float)
        assert(type(data['Marcap']) == float)
        assert(type(data['MarcapRatio']) == float)
        assert(type(data['Stocks']) == float)
        if data['ForeignShares']:
            assert(type(data['ForeignShares']) == float)
        else:
            assert(data['ForeignShares'] == '')

        if data['ForeignRatio']:
            assert(type(data['ForeignRatio']) == float)
        else:
            assert(data['ForeignRatio'] == '')
        assert(type(data['Rank']) == float)
        assert(type(data['Date']) == str)