import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 上海市环境保护局
# http://www.sepb.gov.cn/hb/fa/cms/shhj/zhifa_login.jsp
# http://www.sepb.gov.cn/fa/cms/shhj/shhj2060/shhj5300/index.shtml
# http://www.sepb.gov.cn/fa/cms/shhj/shhj2060/shhj5302/index.shtml
url_prefix = 'http://www.sepb.gov.cn'
gov_name = '上海市环境保护局'
collection_name = 'environment_data'

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


def get_list():
    result_list = []
    url = 'http://www.sepb.gov.cn/zhifa/law_enforce_list.jsp'
    response = request_site_page(url)
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    soup = bs(response.content if response else '', 'lxml')
    page_count = int(soup.find(class_='scroll').find_all_next(class_='bold_nun')[1].text)
    logger.info('{} 一共有{}页'.format(gov_name, page_count))
    for num in range(page_count):
        try:
            response = request_site_page(url=url, methods='post', data={'pageNo': num + 1})
            if response is None:
                logger.error('网页请求错误{}'.format(url))
            soup = bs(response.content if response else '', 'lxml')
            for x in soup.find_all('a', class_='listInfo'):
                publish_date = re.search(r'\d+-\d+-\d+', x.attrs['onclick']).group()
                anc_url = 'http://www.sepb.gov.cn/zhifa/law_enforce_sublist.jsp?time=' + publish_date
                if db[collection_name].count_documents({'url': anc_url}) != 0:
                    return
                info = {
                    'title': x.text,
                    'publishDate': publish_date,
                    'url': anc_url,
                    'type': '行政处罚决定',
                    'origin': gov_name,
                    'status': 'not parsed'
                }
                logger.info('{} 新公告：{}'.format(gov_name, info['title']))
                if info not in result_list:
                    result_list.append(info)
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


def crawler(channelId):
    result_list = []
    url = 'http://www.sepb.gov.cn/hb/fa/cms/shhj/list_login.jsp?channelId=' + channelId
    response = request_site_page(url)
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    soup = bs(response.content if response else '', 'lxml')
    page_count = int(soup.find(class_='scroll').find_all_next(class_='bold_nun')[1].text)
    logger.info('{} 一共有{}页'.format(gov_name, page_count))
    for num in range(page_count):
        try:
            response = request_site_page(url=url, methods='post', data={'pageNo': num + 1})
            if response is None:
                logger.error('网页请求错误{}'.format(url))
            soup = bs(response.content if response else '', 'lxml')
            for x in soup.find(class_='ascend_ul').find_all('a'):
                anc_url = x.attrs['href'].strip()
                if not anc_url.startswith('http'):
                    anc_url = urljoin(url_prefix, anc_url)
                if db[collection_name].count_documents({'url': anc_url}) != 0:
                    return
                publish_date = x.span.text
                x.span.decompose()
                info = {
                    'title': x.text,
                    'publishDate': publish_date,
                    'url': anc_url,
                    'type': '行政处罚决定',
                    'origin': gov_name,
                    'status': 'not parsed'
                }
                logger.info('{} 新公告：{}'.format(gov_name, info['title']))
                if info not in result_list:
                    result_list.append(info)
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


if __name__ == '__main__':
    get_list()
    crawler('5300')
    crawler('5302')
