import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 浙江省税务局
# http://www.zjtax.gov.cn/col/col9217/index.html
# http://zhejiang.chinatax.gov.cn/col/col10674/index.html
# http://www.zjzwfw.gov.cn/zjzw/punish/frontpunish/showadmins.do?webId=1
first_url = 'http://zhejiang.chinatax.gov.cn/module/jslib/bulletin/ajaxdata.jsp'
second_url = 'http://zhejiang.chinatax.gov.cn/module/xxgk/search.jsp'
third_url = 'http://www.zjzwfw.gov.cn/zjzw/punish/frontpunish/punish_list.do?' \
            'deptId=001003028&webid=1&xzcf_code=&pageNo='
gov_name = '浙江省税务局'
collection_name = 'tax_data'

area_name = [
    '杭州',
    '温州',
    '绍兴',
    '嘉兴',
    '湖州',
    '金华',
    '衢州',
    '台州',
    '舟山',
    '丽水'
]
params = {
    'col': 1,
    'startrecord': 1,
    'endrecord': 32,
    'perpage': 11,
    'rowpage': 1,
    'searhvalue': area_name[0],
    'searchkey': 'area',
    'year': ''
}
params2 = {
    'divid': 'div10674',
    'infotypeId': 'Z0710',
    'jdid': 15,
    'area': '11330000002484088Y',
    'sortfield': 'createdatetime:0',
    'currpage': 1
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
    response = request_site_page(first_url, params=params, methods='post')
    response.encoding = response.apparent_encoding
    if response is None:
        logger.error('网页请求错误{}'.format(first_url))
    for each_area in area_name:
        try:
            stop_flag = False
            if db.crawler.find({'url': each_area + first_url}).count() > 0:
                last_updated_url = db.crawler.find_one({'url': each_area + first_url})['last_updated']
            else:
                last_updated_url = ''
            params['searhvalue'] = each_area
            response = request_site_page(first_url, params=params, methods='post')
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
            data_raw = bs(re.search('dataStore =([\s\S]*);', str(soup)).group(1).strip(), 'lxml')
            data_list = data_raw.find_all('tr')
            for index, each_data in enumerate(data_list):
                href = each_data.find('a')['href']
                anc_url = urljoin(first_url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0:
                    if db.crawler.find({'url': each_area + first_url}).count() > 0:
                        if db.crawler.find_one({'url': each_area + first_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': each_area + first_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': each_area + first_url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_data.find('a').text.strip()
                publish_date = re.search('(\d+/\d+/\d+)', href).group(1).strip().replace('/', '-')

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
                continue
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    response = request_site_page(second_url, methods='post', params=params2)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(second_url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': second_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': second_url})['last_updated']
    else:
        last_updated_url = ''
    page_count = re.findall('共(\d+)页', soup.text)[-1]
    page_num = 1
    while page_num <= int(page_count):
        logger.info('第%d页' % page_num)
        try:
            data_list = soup.find_all(attrs={"class": "tr_main_value_odd"}) \
                        + soup.find_all(attrs={"class": "tr_main_value_even"})
            for index, each_data in enumerate(data_list):
                title = each_data.find('a')['mc']
                if re.search('更多数据', title):
                    continue
                href = each_data.find('a')['href']
                anc_url = urljoin(second_url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if page_num == 1 and index == 0:
                    if db.crawler.find({'url': second_url}).count() > 0:
                        if db.crawler.find_one({'url': second_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': second_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': second_url, 'last_updated': anc_url, 'origin': gov_name})
                publish_date = each_data.find('a')['rq']

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
            params2['currpage'] = page_num
            response = request_site_page(second_url, methods='post', params=params2)
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    url = third_url + '1'
    response = request_site_page(url)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': third_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': third_url})['last_updated']
    else:
        last_updated_url = ''
    page_count = re.findall('共(\d+)页', soup.text)[-1]
    page_num = 1
    while page_num <= int(page_count):
        logger.info('第%d页' % page_num)
        try:
            data_list = soup.find(attrs={"id": "xzcf_4"}).find_all('tr')
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
                        db.crawler.insert_one(
                            {'url': third_url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_data.find('a').text.strip()
                publish_date = each_data.find_all('td')[-1].text.strip()

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
            url = third_url + str(page_num)
            response = request_site_page(url)
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
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
