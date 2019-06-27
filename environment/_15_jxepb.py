import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 江西省环境保护厅
# http://www.jxepb.gov.cn/ZWGK/ZTZL/wryhjjgxx/xzcf/zjzcdcfjd/index.htm
# http://www.jxepb.gov.cn/ZWGK/ZTZL/wryhjjgxx/xzcf/qyhphpb/index.htm
url_format_list = [
    'http://www.jxepb.gov.cn/ZWGK/ZTZL/wryhjjgxx/xzcf/zjzcdcfjd/index{}.htm',
    'http://www.jxepb.gov.cn/ZWGK/ZTZL/wryhjjgxx/xzcf/qyhphpb/index{}.htm',
]
url_prefix = 'http://www.jxepb.gov.cn'
gov_name = '江西省环境保护厅'
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


def crawler(url_format):
    result_list = []
    response = request_site_page(url_format.format(''))
    if response is None:
        logger.error('网页请求错误{}'.format(url_format.format('')))
    page_count = int(re.search(r'javascript:prePage\((\d+)', response.text if response else '').group(1))
    logger.info('{} 一共有{}页'.format(gov_name, page_count))

    for num in range(page_count):
        url = url_format.format(str(page_count - num) if num != 0 else '')
        try:
            response = request_site_page(url)
            if response is None:
                logger.error('网页请求错误{}'.format(url))
            soup = bs(response.content if response else '', 'lxml')
            for x in soup.find_all(id='zy-wan-ts'):
                anc_url = x.find('a').attrs['href'].strip()
                if not anc_url.startswith('http'):
                    anc_url = urljoin(url_format.format(''), anc_url)
                if db[collection_name].count_documents({'url': anc_url}) != 0:
                    return
                info = {
                    'title': x.find('a').attrs['title'].strip(),
                    'publishDate': x.find(id='zy-wan-s').text,
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
    for x in url_format_list:
        crawler(x)
