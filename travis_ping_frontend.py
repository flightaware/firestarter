from selenium import webdriver

driver = webdriver.PhantomJS()
driver.get('http://localhost:5000/')
sleep(3)
page_output = driver.page_source
print(page_output)
driver.quit()