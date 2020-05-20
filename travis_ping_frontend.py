import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.binary_location = '/usr/bin/google-chrome'

driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(),   chrome_options=chrome_options)
driver.get('http://localhost:5000/')
time.sleep(30)
page_output = driver.page_source
print(page_output)
driver.quit()