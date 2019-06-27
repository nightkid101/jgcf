import copy
import re

import math
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 辽宁省工商行政管理局
# http://portal.lncredit.gov.cn/TestUsers/website/ln210000/wsreportingdoublepublicityquery/penaltyindex#
url = 'http://portal.lncredit.gov.cn/TestUsers/website/ln210000/wsreportingdoublepublicityquery/getPenaltyPage'
gov_name = '辽宁省工商行政管理局'
collection_name = 'industry_commerce_data'

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
param = {
    'pageNumber': 1,
    'pageSize': 20,
}
header = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest'
}


def crawler():
    result_list = []
    response = request_site_page(url, methods='post', params=param, headers=header)
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    if response is None:
        return
    data = response.json()
    page_count = math.ceil(data['total'] / 20)
    logger.info('{} 一共有{}页'.format(gov_name, page_count))

    for num in range(page_count):
        temp = copy.deepcopy(param)
        temp['pageNumber'] = num + 1
        try:
            response = request_site_page(url, methods='post', params=temp, headers=header)
            if response is None:
                logger.error('网页请求错误{}'.format(url))
                return
            response.encoding = 'utf8'
            for x in response.json()['rows']:
                anc_url = 'http://portal.lncredit.gov.cn/TestUsers/website/ln210000/wsreportingdoublepublicityquery/punishdetail?id='
                anc_url += x['id']
                publish_date = re.search(r'\d+\-\d+\-\d+', x['uploadtime']).group()
                if db[collection_name].count_documents({'url': anc_url}) != 0:
                    return
                info = {
                    'title': x['punishname'].strip(),
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
    crawler()
