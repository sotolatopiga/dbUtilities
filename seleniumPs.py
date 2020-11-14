#%%  https://stackoverflow.com/questions/20907180/getting-console-log-output-from-chrome-with-selenium-python-api-bindings
#
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from time import sleep

# enable browser logging
d = DesiredCapabilities.CHROME
d['goog:loggingPrefs'] = { 'browser':'ALL' }
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
# options.add_argument("start-maximized")
options.add_argument("--window-size=1920,1080")
options.add_argument("--auto-open-devtools-for-tabs")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
driver = webdriver.Chrome(desired_capabilities=d,options=options)

# load the desired webpage
driver.get('https://banggia.vps.com.vn/#PhaiSinh/VN30')
sleep(3)
driver.execute_script("""document.querySelector("#sym_VN30F2011 > a").click()""")
sleep(1)
driver.get_screenshot_as_file("/home/sotola/Desktop/screenshot.png")
# https://stackoverflow.com/questions/20907180/getting-console-log-output-from-chrome-with-selenium-python-api-bindings
sleep(1)

with open('scrape_psPressure.js', 'r') as file:
    scrapeCode = file.read()
    scrapeCode = scrapeCode.replace("FLASK_PORT = 5000", "FLASK_PORT = 5005")

driver.execute_script(scrapeCode)

def log(): [print(entry) for entry in driver.get_log('browser')]

#%%
