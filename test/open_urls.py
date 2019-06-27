from pymongo import MongoClient
from init import logger_init, config_init
from selenium import webdriver
config = config_init()
if config['mongodb']['dev_mongo'] == '1':
    db = MongoClient(config['mongodb']['ali_mongodb_url'], username=config['mongodb']['ali_mongodb_username'],
                     password=config['mongodb']['ali_mongodb_password'],
                     port=int(config['mongodb']['ali_mongodb_port']))[config['mongodb']['ali_mongodb_name']]
else:
    db = MongoClient(
        host=config['mongodb']['mongodb_host'],
        port=int(config['mongodb']['mongodb_port']),
        username=None if config['mongodb']['mongodb_username'] == '' else config['mongodb']['mongodb_username'],
        password=None if config['mongodb']['mongodb_password'] == '' else config['mongodb']['mongodb_password'])[
        config['mongodb']['mongodb_db_name']]

def open_urls():#{'$nin': ['ignored']}
    num = 10
    start = 0
    browser = webdriver.Chrome()
    browser.set_page_load_timeout(20)
    for each_document in db.environment_data.find({'origin': '海南省环境保护厅', 'status': 'not parsed'}):
        announcement_url = each_document['url']
        js = 'window.open("' + announcement_url + '");'
        browser.execute_script(js)
        start = start + 1
        if start > num:
            break
    a = 0

def set_parsed():
    # num = 10
    # start = 0
    for each_document in db.environment_data.find({'origin': '湖北省环境保护厅', 'status': 'ignored'}):
        db.environment_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'not parsed'}})
        # start += 1
        # if start > num:
        #     return

if __name__=='__main__':
    open_urls()
    # set_parsed()