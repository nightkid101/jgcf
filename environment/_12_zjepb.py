import copy
import re

from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 浙江省环境保护厅
# http://www.zjepb.gov.cn/col/col1201446/index.html
url = 'http://www.zjepb.gov.cn/module/jpage/dataproxy.jsp'
gov_name = '浙江省环境保护厅'
collection_name = 'environment_data'
param = {
    'startrecord': 1,
    'endrecord': 20,
    'perpage': 20
}
data = {
    'col': 1,
    'appid': 1,
    'webid': 1756,
    'path': '/',
    'columnid': 1201446,
    'sourceContentType': 1,
    'unitid': 3969356,
    'webname': '浙江省生态环境厅',
    'permissiontype': 0
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
    response = request_site_page(url, methods='post', params=param, data=data)
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    page_count = 0
    result = re.search(r'<totalpage>(\d+)', response.text if response else '')
    if result:
        page_count = int(result.group(1))
    logger.info('{} 一共有{}页'.format(gov_name, page_count))
    for num in range(page_count):
        try:
            param_temp = copy.deepcopy(param)
            param_temp['startrecord'] = num * 20 + 1
            param_temp['endrecord'] = (num + 1) * 20
            response = request_site_page(url, methods='post', params=param_temp, data=data)
            if response is None:
                logger.error('网页请求错误{}'.format(url))
            for x in re.findall(r'href=\'(.*?)\' title=\'(.*?)\'.*?\[(.*?)\]', response.text if response else ''):
                anc_url = 'http://www.zjepb.gov.cn' + x[0].strip()
                if db[collection_name].count_documents({'url': anc_url}) != 0:
                    return
                info = {
                    'title': x[1],
                    'publishDate': x[2],
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
