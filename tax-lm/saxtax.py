import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page
import json

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1440,900')
chrome_options.add_argument('--silent')
chrome_options.add_argument('lang=zh_CN.UTF-8')
chrome_options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36')
from time import sleep

# 山西省税务局
# http://shanxi.chinatax.gov.cn/topic/list/sx-11400-3840-2961
first_url = 'http://shanxi.chinatax.gov.cn/topic/list/sx-11400-3840-2961'
gov_name = '山西省税务局'
collection_name = 'tax_data'

logger = logger_init(gov_name)
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

db[collection_name].create_index([('url', 1)])


def crawler():
    result_list = []
    browser = webdriver.Chrome(chrome_options=chrome_options)
    browser.set_page_load_timeout(50)
    page_num = 1
    browser.get(first_url)
    wait = WebDriverWait(browser, 50)
    wait.until(EC.presence_of_element_located((By.ID, 'wzList')))
    sleep(5)
    response = browser.page_source
    stop_flag = False
    soup = bs(response if response else '', 'lxml')

    if db.crawler.find({'url': first_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': first_url})['last_updated']
    else:
        last_updated_url = ''
    #获取总页码
    tmp_response = request_site_page('http://shanxi.chinatax.gov.cn/common/extQuery?sqlid=zt_data_wz2&limit=15&zid=3840&cid=2961&orgid=11400&page=1')
    tmp_soup = bs(tmp_response.text, 'lxml')
    json_data = json.loads(tmp_soup.body.text)['message']
    page_count = int(json_data['totalPage'])
    while page_num <= page_count:
        logger.info('第%d页' % page_num)
        try:
            info_list = soup.find('div', class_='morelist_l_con').find_all('li')
            for index, each_info in enumerate(info_list):
                anc_url = urljoin(first_url, each_info.find('a')['href'])

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if page_num == 1 and index == 0:
                    if db.crawler.find({'url': first_url}).count() > 0:
                        if db.crawler.find_one({'url': first_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': first_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': first_url, 'last_updated': anc_url, 'origin': gov_name})
                publish_date = each_info.find('span').text.strip()
                title = each_info.find('p').text.strip()

                if db[collection_name].count_documents({'url': anc_url}) == 0:
                    info = {
                        'title': title,
                        'publishDate': publish_date,
                        'url': anc_url,
                        'type': '行政处罚决定',
                        'origin': gov_name,
                        'status': 'not parsed'
                    }
                    logger.info('{} 新公告：{} url: {}'.format(gov_name, info['title'], anc_url))
                    if info not in result_list:
                        result_list.append(info)
                else:
                    if config['crawler_update_type']['update_type'] == '0':
                        break
            if stop_flag:
                logger.info('到达上次爬取的链接')
                break
            page_num += 1
            if page_num > page_count:
                break
            button = browser.find_element_by_css_selector('[paged="'+str(page_num)+'"]')
            button.click()
            wait = WebDriverWait(browser, 50)
            wait.until(EC.presence_of_element_located((By.ID, 'wzList')))
            sleep(5)
            response = browser.page_source
            soup = bs(response if response else '', 'lxml')
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue

    if len(result_list) > 0:
        logger.info('{}一共有{}条新公告，导入数据库中......'.format(gov_name, len(result_list)))
        r = db[collection_name].insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('{}公告导入完成！'.format(gov_name))
        else:
            logger.error('{}公告导入出现问题！'.format(gov_name))
    else:
        logger.info('{}没有新公告！'.format(gov_name))
    browser.close()


if __name__ == '__main__':
    crawler()
