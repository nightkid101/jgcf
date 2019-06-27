import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 山东省税务局
# http://www.sd-n-tax.gov.cn/col/col62935/index.html
# http://www.sd-n-tax.gov.cn/col/col62973/index.html
# http://old.sd-n-tax.gov.cn/col/col56490/index.html  # 无数据
first_url = 'http://www.sd-n-tax.gov.cn/module/jslib/bulletin/ajaxdata.jsp'
second_url = 'http://www.sd-n-tax.gov.cn/col/col62973/index.html'

gov_name = '山东省税务局'
collection_name = 'tax_data'

area_list = [
    {'year': '2018年度', 'value': '第一季度'},
    {'year': '2018年度', 'value': '第二季度'},
    {'year': '2018年度', 'value': '第三季度'},
    {'year': '2018年度', 'value': '第四季度'},
    {'year': '2019年度', 'value': '第一季度'},
    {'year': '2019年度', 'value': '第二季度'},
    {'year': '2019年度', 'value': '第三季度'},
    {'year': '2019年度', 'value': '第四季度'},
]
params = {
    'startrecord': 1,
    'endrecord': 5,
    'perpage': 15,
    'searhvalue': area_list[0]['value'],
    'searchkey': 'jd',
    'year': area_list[0]['year'],
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
    for each_area in area_list:
        logger.info(each_area['year'] + each_area['value'])
        params['searhvalue'] = each_area['value']
        params['year'] = each_area['year']
        response = request_site_page(first_url, methods='post', params=params)
        response.encoding = response.apparent_encoding
        if response is None:
            logger.error('网页请求错误{}'.format(first_url))
        soup = bs(response.content if response else '', 'lxml')

        if db.crawler.find({'url': each_area['year'] + each_area['value'] + first_url}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': each_area['year'] + each_area['value'] + first_url})['last_updated']
        else:
            last_updated_url = ''
        try:
            data_list = soup.find_all('tr')
            for index, each_data in enumerate(data_list):
                anc_url = each_data.find('a')['href']

                if anc_url == last_updated_url:
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0:
                    if db.crawler.find({'url': each_area['year'] + each_area['value'] + first_url}).count() > 0:
                        if db.crawler.find_one({'url': each_area['year'] + each_area['value'] + first_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': each_area['year'] + each_area['value'] + first_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': each_area['year'] + each_area['value'] + first_url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_data.find('a').text.strip()
                publish_date = re.search('(\d+/\d+/\d+)', anc_url).group(1).strip().replace('/', '-')

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
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    response = request_site_page(second_url)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(second_url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': second_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': second_url})['last_updated']
    else:
        last_updated_url = ''
    try:
        data_list = bs(soup.find(attrs={"class": "btlb"}).text, 'lxml').find_all('li')
        for index, each_data in enumerate(data_list):
            href = each_data.find('a')['href']
            anc_url = urljoin(second_url, href)

            if anc_url == last_updated_url:
                stop_flag = True
                logger.info('到达上次爬取的链接')
                break
            if index == 0:
                if db.crawler.find({'url': second_url}).count() > 0:
                    if db.crawler.find_one({'url': second_url})['last_updated'] != anc_url:
                        db.crawler.update_one({'url': second_url}, {'$set': {'last_updated': anc_url}})
                else:
                    db.crawler.insert_one(
                        {'url': second_url, 'last_updated': anc_url, 'origin': gov_name})
            title = each_data.find('a')['title']
            publish_date = each_data.find('span').text.strip()

            if re.search('(欠税|非正常)', title):
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


if __name__ == '__main__':
    crawler()
