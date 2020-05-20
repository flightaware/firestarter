from selenium import webdriver
from chromedriver_py import binary_path
import time

driver = webdriver.Chrome(executable_path=binary_path)  
driver.get('http://localhost:5000/')
time.sleep(30)
page_output = driver.page_source
print(page_output)
driver.quit()