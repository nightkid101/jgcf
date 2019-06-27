import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 黑龙江省税务局
# http://www.hl-n-tax.gov.cn/module/jslib/bulletin/zdss.html
# http://heilongjiang.chinatax.gov.cn/col/col6852/index.html
first_url = 'http://heilongjiang.chinatax.gov.cn/module/jslib/bulletin/ajaxdata.jsp?' \
            'startrecord=1&endrecord=21&perpage=11'
second_url = 'http://heilongjiang.chinatax.gov.cn/module/xxgk/search.jsp?divid=div6852&infotypeId=G103&jdid=18&area='
gov_name = '黑龙江省税务局'
collection_name = 'tax_data'

area_value = [
    '%E9%BD%90%E9%BD%90%E5%93%88%E5%B0%94%E5%B8%82',    # 齐齐哈尔市
    '%E5%93%88%E5%B0%94%E6%BB%A8%E5%B8%82',             # 哈尔滨市
    '%E4%BD%B3%E6%9C%A8%E6%96%AF%E5%B8%82',             # 佳木斯市
    '%E4%BC%8A%E6%98%A5%E5%B8%82',                      # 伊春市
    '%E4%B8%83%E5%8F%B0%E6%B2%B3%E5%B8%82'              # 七台河市
]
data = {
    'searhvalue': area_value[0],
    'searchkey': 'area'
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
    if response is None:
        logger.error('网页请求错误{}'.format(first_url))
    for index, each_value in enumerate(area_value):
        stop_flag = False
        if db.crawler.find({'url': each_value + first_url}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': each_value + first_url})['last_updated']
        else:
            last_updated_url = ''
        logger.info('第%d个地区' % (index+1))
        try:
            data['searhvalue'] = each_value
            response = request_site_page(first_url, methods='post', params=data)
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
            data_list = soup.find_all('tr')
            for index2, each_data in enumerate(data_list):
                href = each_data.find('a')['href']
                anc_url = urljoin(first_url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index2 == 0:
                    if db.crawler.find({'url': each_value + first_url}).count() > 0:
                        if db.crawler.find_one({'url': each_value + first_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': each_value + first_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': each_value + first_url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_data.find('a').text.strip()
                publish_date = re.search('(\d+/\d+/\d+)', href).group(1).replace('/', '-').strip()

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
    response = request_site_page(second_url, params=data, methods='post')
    response.encoding = response.apparent_encoding
    soup = bs(response.content if response else '', 'lxml')
    if response is None:
        logger.error('网页请求错误{}'.format(second_url))

    data_list = soup.find_all(attrs={"class", "tr_main_value_odd"}) + soup.find_all(
                             attrs={"class": "tr_main_value_even"})
    stop_flag = False
    if db.crawler.find({'url': second_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': second_url})['last_updated']
    else:
        last_updated_url = ''
    for index, each_data in enumerate(data_list):
        href = each_data.find('a')['href']
        anc_url = urljoin(second_url, href)
        if anc_url == last_updated_url:
            stop_flag = True
            logger.info('到达上次爬取的链接')
            break
        if db.crawler.find({'url': second_url}).count() > 0:
            if db.crawler.find_one({'url': second_url})['last_updated'] != anc_url:
                db.crawler.update_one({'url': second_url}, {'$set': {'last_updated': anc_url}})
        else:
            db.crawler.insert_one({'url': second_url, 'last_updated': anc_url, 'origin': gov_name})
        title = each_data.find('a')['mc']
        publish_date = re.search('(\d+/\d+/\d+)', href).group(1).replace('/', '-').strip()

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
