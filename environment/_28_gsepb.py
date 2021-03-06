import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 甘肃省环境保护厅
# http://www.gsep.gansu.gov.cn/govinfoclass.jsp?ainfolist1085t=2&ainfolist1085p=1&ainfolist1085c=15&urltype=egovinfo.EgovTreeURl&subtype=1&wbtreeid=1264&type=egovinfosubcattree&sccode=I&gilevel=1
url = 'http://www.gsep.gansu.gov.cn/govinfoclass.jsp'
gov_name = '甘肃省环境保护厅'
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

param = {
    'ainfolist1085t': 2,
    'ainfolist1085p': 1,
    'ainfolist1085c': 15,
    'urltype': 'egovinfo.EgovTreeURl',
    'subtype': 1,
    'wbtreeid': 1264,
    'type': 'egovinfosubcattree',
    'sccode': 'I',
    'gilevel': 1
}


def crawler():
    result_list = []
    response = request_site_page(url, params=param)
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    soup = bs(response.content if response else '', 'lxml')
    page_count_text = soup.body.find(class_='fanyestyle1085').tr.td
    page_count = int(re.findall(r'\d+/(\d+)', str(page_count_text) if page_count_text else '')[-1])
    logger.info('{} 一共有{}页'.format(gov_name, page_count))

    for num in range(page_count):
        param['ainfolist1085p'] = num + 1
        try:
            response = request_site_page(url, params=param)
            if response is None:
                logger.error('网页请求错误{}'.format(url))
            soup = bs(response.content if response else '', 'lxml')
            for i, x in enumerate(soup.body.find(class_='govinfolist1085').find_all('tr')):
                if i == 0:
                    continue
                td_list = x.find_all('td')
                anc_url = td_list[1].a.attrs['href'].strip()
                if not anc_url.startswith('http'):
                    anc_url = urljoin(url, anc_url)
                if db[collection_name].count_documents({'url': anc_url}) != 0:
                    return
                info = {
                    'title': td_list[1].a.text.strip(),
                    'publishDate': td_list[2].text.strip().replace('年', '-').replace('月', '-').replace('日', ''),
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
