import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 上海市税务局
# http://www.tax.sh.gov.cn/pub/xxgk/ssgg/
# http://www.tax.sh.gov.cn/xbwz/tycx/TYCXzdsswfajgblCtrl-init.pfv#
# https://www.tax.sh.gov.cn/xbwz/wzcx/QYDA_qyda03_xzcfajxxcx.jsp?swjgDm=13100000000  暂无内容
first_url = 'http://www.tax.sh.gov.cn/pub/xxgk/ssgg/index{}.html'
second_url = 'http://www.tax.sh.gov.cn/xbwz/tycx/TYCXzdsswfajgblCtrl-getxxsByTj.pfv?swjgdm='
gov_name = '上海市税务局'
collection_name = 'tax_data'

address_id = [
    '13101150000',  # 浦东新区税务局
    '13101010000',  # 黄浦区税务局
    '13101040000',  # 徐汇区税务局
    '13101060000',  # 静安区税务局
    '13101050000',  # 长宁区税务局
    '13101070000',  # 普陀区税务局
    '13101090000',  # 虹口区税务局
    '13101100000',  # 杨浦区税务局
    '13101130000',  # 宝山区税务局
    '13101120000',  # 闵行区税务局
    '13101140000',  # 嘉定区税务局
    '13102280000',  # 金山区税务局
    '13102270000',  # 松江区税务局
    '13102290000',  # 青浦区税务局
    '13102260000',  # 奉贤区税务局
    '13102300000',  # 崇明区税务局
    '13101410000',  # 保税区税务局
]

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
    response = request_site_page(first_url.format(''))
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(first_url.format('')))

    if db.crawler.find({'url': first_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': first_url})['last_updated']
    else:
        last_updated_url = ''
    soup = bs(response.content if response else '', 'lxml')
    page_count = re.search('var pagecount = (\d+);', soup.text.strip()).group(1).strip()
    page_num = 0
    while page_num < int(page_count):
        try:
            data_list = soup.find_all('dd')
            for index, each_data in enumerate(data_list):
                title = each_data.find('a')['title']
                href = each_data.find('a')['href']
                anc_url = urljoin(first_url.format('') if page_num == 0 else first_url.format('_' + str(page_num)), href)
                if page_num == 0 and index == 0:
                    if db.crawler.find({'url': first_url}).count() > 0:
                        if db.crawler.find_one({'url': first_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': first_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': first_url, 'last_updated': anc_url, 'origin': gov_name})
                if re.search('稽查局送达公告', title):
                    if anc_url == last_updated_url:
                        stop_flag = True
                        logger.info('到达上次爬取的链接')
                        break

                    publish_date = each_data.find('span').text.strip()

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
            response = request_site_page(first_url.format('_' + str(page_num)))
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    base_url = 'http://www.tax.sh.gov.cn/xbwz/tycx/TYCXzdsswfajgblCtrl-getxxByNsrsbm.pfv?djxh='
    for index, each_address in enumerate(address_id):
        url = second_url + each_address + '&qjswjgdm=&curPage=' + '1'
        response = request_site_page(url)
        response.encoding = response.apparent_encoding

        stop_flag = False
        if response is None:
            logger.error('网页请求错误{}'.format(url))

        if db.crawler.find({'url': str(index) + url}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': str(index) + url})['last_updated']
        else:
            last_updated_url = ''

        soup = bs(response.content if response else '', 'lxml')
        json_data = json.loads(soup.text, strict=False)
        has_next = json_data['nextPageEnable']
        page_num = 1
        while True:
            data_list = json_data['pageData']
            for index2, each_data in enumerate(data_list):
                article_id = each_data['djxh']
                anc_url = base_url + str(article_id)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if page_num == 1 and index2 == 0:
                    if db.crawler.find({'url': str(index) + url}).count() > 0:
                        if db.crawler.find_one({'url': str(index) + url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': str(index) + url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': str(index) + url, 'last_updated': anc_url, 'origin': gov_name})
                publish_date = each_data['wjtime']
                title = each_data['nsrmc']

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
            if has_next is False:
                break
            page_num += 1
            url = second_url + each_address + '&qjswjgdm=&curPage=' + str(page_num)
            r = request_site_page(url)
            r.encoding = r.apparent_encoding
            soup = bs(r.content if r else '', 'lxml')
            json_data = json.loads(soup.text, strict=False)
            has_next = json_data['nextPageEnable']
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
