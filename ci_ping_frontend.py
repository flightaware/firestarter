import os
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

class NoAirportsFound(Exception):
    pass

time.sleep(30)

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.binary_location = '/usr/bin/google-chrome'

driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(),   chrome_options=chrome_options)
driver.get('http://localhost:8080/')
page_output = driver.page_source

p = re.compile("airport-list-link.*?>([A-Z]{4})")
airports = p.findall(page_output)

if len(airports) > 0:
	print(airports)
else:
	raise NoAirportsFound()

driver.quit()
