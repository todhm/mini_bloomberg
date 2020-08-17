
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

def return_download_path_driver(download_path='/usr/src/app/datacollections'):
    agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    prefs = {
        "download.default_directory": download_path,
        "profile.default_content_settings.popups": 0,
        "directory_upgrade": True,
        "browser.set_download_behavior": 'allow'
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-blink-features")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('window-size=1920x1080')
    chrome_options.add_argument(f'--user-agent={agent}')
    chrome_options.add_argument("--log-level=1")
    chrome_options.add_argument("--enable-logging --v=1")
    capabilities = chrome_options.to_capabilities()
    d = DesiredCapabilities.CHROME
    d.update(capabilities)
    d['loggingPrefs'] = {'browser': 'ALL'}
    driver = webdriver.Chrome(
        chrome_options=chrome_options,
        desired_capabilities=d
    )
    driver.execute_cdp_cmd(
        "Network.setExtraHTTPHeaders", 
        {"headers": {"User-Agent": "browserClientA"}}
    )
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
            })
        """
    })
    driver.execute_cdp_cmd(
        'Network.setUserAgentOverride', 
        {
            "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
        }
    )
    return driver

#  2010년 1월부터시작
