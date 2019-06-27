import copy
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 安徽省环境保护厅
# http://sthjt.ah.gov.cn/pages/XXGKList.aspx?MenuID=120200
# http://sthjt.ah.gov.cn/pages/XXGKList.aspx?MenuID=120300
url = 'http://sthjt.ah.gov.cn/pages/XXGKList.aspx'
url_prefix = 'http://sthjt.ah.gov.cn/pages/'
gov_name = '安徽省环境保护厅'
collection_name = 'environment_data'

param = {
    'MenuID': 120200,
    'page': 1
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


def crawler(MenuID):
    result_list = []
    param_temp = copy.deepcopy(param)
    param_temp['MenuID'] = MenuID
    response = request_site_page(url, params=param_temp)
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    soup = bs(response.content if response else '', 'lxml')
    page_count_text = soup.body.find(class_='pages')
    pattern_result = re.findall(r'共<font color="red">(\d+)', str(page_count_text) if page_count_text else '')
    page_count = int(pattern_result[-1]) if len(pattern_result) > 0 else 1
    logger.info('{} 一共有{}页'.format(gov_name, page_count))

    for num in range(page_count):
        param_temp['page'] = num + 1
        try:
            response = request_site_page(url, params=param_temp)
            if response is None:
                logger.error('网页请求错误{}'.format(url))
            soup = bs(response.content if response else '', 'lxml')
            for x in soup.find(class_='t_infos_c').find_all('li'):
                anc_url = x.a.attrs['href'].strip()
                if not anc_url.startswith('http'):
                    anc_url = urljoin(url_prefix, anc_url)
                if db[collection_name].count_documents({'url': anc_url}) != 0:
                    return
                info = {
                    'title': x.a.attrs['title'].strip(),
                    'publishDate': x.cite.text.strip(),
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
    crawler(120200)
    crawler(120300)
