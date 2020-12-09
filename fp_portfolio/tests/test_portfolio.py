

def test_link_fetch_api(test_app):
    response = test_app.get(
        '/'
    )
    result = response.json
    print(result)
