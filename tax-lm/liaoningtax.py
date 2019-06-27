import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

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

# 辽宁省税务局
# http://portal.lncredit.gov.cn/TestUsers/website/ln210000/wsreportingdoublepublicityquery/penaltyindex
# http://liaoning.chinatax.gov.cn/col/col1850/index.html
# http://liaoning.chinatax.gov.cn/col/col1030/index.html
first_url = 'http://portal.lncredit.gov.cn/TestUsers/website/ln210000/wsreportingdoublepublicityquery/getPenaltyPage'
second_url = 'http://liaoning.chinatax.gov.cn/module/search/index.jsp?field=vc_name:1,c_createtime:3,field_495:1,' \
             'field_518:3,field_516:1,field_518:1&i_columnid=style_5&c_createtime_start=&c_createtime_end=&' \
             'field_518_start=&field_518_end=&vc_name=&field_495=&field_516=&field_518=&currpage='
third_url = 'http://liaoning.chinatax.gov.cn/col/col1030/index.html?uid=2970&pageNum={}'
gov_name = '辽宁省税务局'
collection_name = 'tax_data'

data = {
    'pageNumber': 1
}

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
    response = request_site_page(first_url, params=data, methods='post')
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(first_url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': first_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': first_url})['last_updated']
    else:
        last_updated_url = ''
    json_data = json.loads(soup.find('p').text.strip())
    records_count = json_data['total']
    page_count = int(int(records_count) / 10) + 1
    page_num = 1
    base_url = 'http://portal.lncredit.gov.cn/TestUsers/website/ln210000' \
               '/wsreportingdoublepublicityquery/punishdetail?id='
    while page_num <= int(page_count):
        logger.info('第%d页' % page_num)
        try:
            data_list = json_data['rows']
            for index, each_data in enumerate(data_list):
                article_id = each_data['id']
                anc_url = base_url + article_id

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if db.crawler.find({'url': first_url}).count() > 0:
                    if db.crawler.find_one({'url': first_url})['last_updated'] != anc_url:
                        db.crawler.update_one({'url': first_url}, {'$set': {'last_updated': anc_url}})
                else:
                    db.crawler.insert_one(
                        {'url': first_url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_data['punishname']
                publish_date = re.search('(\d+-\d+-\d+)', each_data['uploadtime']).group(1).strip()

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
            data['pageNumber'] = page_num
            response = request_site_page(first_url, params=data, methods='post')
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
            json_data = json.loads(soup.find('p').text.strip())
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    url = second_url + '1'
    response = request_site_page(url)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': second_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': second_url})['last_updated']
    else:
        last_updated_url = ''
    page_count = re.search('共 (\d+) 页', soup.text).group(1).strip()
    page_num = 1
    while page_num <= int(page_count):
        logger.info('第%d页' % page_num)
        try:
            data_list = soup.find_all('tr')
            for index, each_data in enumerate(data_list):
                if not each_data.find('td'):
                    continue
                href = each_data.find('a')['href']
                anc_url = urljoin(url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0 and page_num == 1:
                    if db.crawler.find({'url': second_url}).count() > 0:
                        if db.crawler.find_one({'url': second_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': second_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': second_url, 'last_updated': anc_url, 'origin': gov_name})

                title = each_data.find_all('li')[1]['title']
                publish_date_text = each_data.find_all('li')[-1].text.strip()
                publish_date = re.search('(\d+-\d+-\d+)', publish_date_text).group(1).strip()

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
            url = second_url + str(page_num)
            response = request_site_page(url)
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue

    page_num = 1
    browser = webdriver.Chrome(chrome_options=chrome_options)
    browser.set_page_load_timeout(50)
    browser.get(third_url.format(str(page_num)))
    wait = WebDriverWait(browser, 50)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'default_pgContainer')))
    r = browser.page_source
    soup = bs(r, 'lxml')

    stop_flag = False
    if db.crawler.find({'url': third_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': third_url})['last_updated']
    else:
        last_updated_url = ''

    page_count = int(soup.find('span', class_ = 'default_pgTotalPage').text)
    while page_num <= page_count:
        try:
            data_list = soup.find(class_='default_pgContainer').find_all('table',
                                                                         style='border-bottom:1px dashed #e8e8e8')
            for index, each_data in enumerate(data_list):
                href = each_data.find('a')['href']
                anc_url = urljoin(third_url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break

                if page_num == 1 and index == 0:
                    if db.crawler.find({'url': third_url}).count() > 0:
                        if db.crawler.find_one({'url': third_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': third_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one({'url': third_url, 'last_updated': anc_url, 'origin': gov_name})

                title = each_data.find('a').text.strip()
                publish_date = re.search('(\d{4}-\d{1,2}-\d{1,2})', each_data.text).group(1).strip()

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
            next_url = urljoin(third_url, soup.find('a', class_ = 'default_pgBtn default_pgNext')['href'])
            browser.get(next_url)
            wait = WebDriverWait(browser, 50)
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'default_pgContainer')))
            r = browser.page_source
            soup = bs(r, 'lxml')

        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')

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
