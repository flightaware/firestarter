from selenium import webdriver
import time

driver = webdriver.PhantomJS()
driver.get('http://localhost:5000/')
time.sleep(3)
page_output = driver.page_source
print(page_output)
driver.quit()