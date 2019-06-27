import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 吉林省税务局
# http://www.jl-n-tax.gov.cn/col/col482/index.html
# http://old.jl-n-tax.gov.cn/col/col6769/index.html
first_url = 'http://www.jl-n-tax.gov.cn/module/jslib/bulletin/ajaxdata.jsp?' \
            'startrecord=1&endrecord=500&perpage=10&rowpage=1'
second_url = 'http://old.jl-n-tax.gov.cn/module/jslib/jquery/jpage/dataproxy.jsp?startrecord={}&endrecord={}&perpage=20'
gov_name = '吉林省税务局'
collection_name = 'tax_data'

data = {
    'searhvalue': 1,
    'searchkey': 'area'
}
data2 = {
    'col': 1,
    'appid': 1,
    'webid': 1,
    'path': '/',
    'columnid': 6769,
    'sourceContentType': 3,
    'unitid': 13457,
    'webname': '国家税务总局吉林省税务局',
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
    # response = request_site_page(first_url, params=data, methods='post')
    # response.encoding = response.apparent_encoding
    # stop_flag = False
    # if response is None:
    #     logger.error('网页请求错误{}'.format(first_url))
    # soup = bs(response.content if response else '', 'lxml')
    #
    # area_num = 1
    # while area_num <= 10:
    #     if db.crawler.find({'url': str(area_num) + first_url}).count() > 0:
    #         last_updated_url = db.crawler.find_one({'url': str(area_num) + first_url})['last_updated']
    #     else:
    #         last_updated_url = ''
    #     logger.info('第%d个地区' % area_num)
    #     try:
    #         data_list = soup.find_all('tr')
    #         for index, each_data in enumerate(data_list):
    #             href = each_data.find('a')['href']
    #             anc_url = urljoin(first_url, href)
    #
    #             if anc_url == last_updated_url:
    #                 stop_flag = True
    #                 logger.info('到达上次爬取的链接')
    #                 break
    #             if index == 0:
    #                 if db.crawler.find({'url': str(area_num) + first_url}).count() > 0:
    #                     if db.crawler.find_one({'url': str(area_num) + first_url})['last_updated'] != anc_url:
    #                         db.crawler.update_one({'url': str(area_num) + first_url}, {'$set': {'last_updated': anc_url}})
    #                 else:
    #                     db.crawler.insert_one(
    #                         {'url': str(area_num) + first_url, 'last_updated': anc_url, 'origin': gov_name})
    #             title = each_data.find('a').text.strip()
    #             publish_date = re.search('(\d+/\d+/\d+)', href).group(1).replace('/', '-').strip()
    #
    #             if db[collection_name].count_documents({'url': anc_url}) == 0:
    #                 info = {
    #                     'title': title,
    #                     'publishDate': publish_date,
    #                     'url': anc_url,
    #                     'type': '行政处罚决定',
    #                     'origin': gov_name,
    #                     'status': 'not parsed'
    #                 }
    #                 logger.info('{} 新公告：{} url: {}'.format(gov_name, info['title'], anc_url))
    #                 if info not in result_list:
    #                     result_list.append(info)
    #             else:
    #                 if config['crawler_update_type']['update_type'] == '0':
    #                     break
    #         area_num += 1
    #         data['searhvalue'] = area_num
    #         response = request_site_page(first_url, params=data, methods='post')
    #         response.encoding = response.apparent_encoding
    #         soup = bs(response.content if response else '', 'lxml')
    #         if stop_flag:
    #             logger.info('到达上次爬取的链接')
    #             continue
    #     except Exception as e:
    #         logger.error(e)
    #         logger.warning('提取公告url出现问题')
    #         continue
    start_record = 1
    end_record = 60
    url = second_url.format(start_record, end_record)
    response = request_site_page(url, params=data2, methods='post')
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': second_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': second_url})['last_updated']
    else:
        last_updated_url = ''
    page_count = soup.totalpage.text.strip()
    page_num = 1
    while page_num <= int(page_count):
        logger.info('第%d页' % page_num)
        try:
            data_list = soup.find_all('record')
            for index, each_data in enumerate(data_list):
                href = each_data.find('a')['href']
                anc_url = urljoin(url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0 and page_num == 1:
                    if db.crawler.find({'url': second_url}).count() > 0:
                        if db.crawler.find_one({'url': second_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': second_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': second_url, 'last_updated': anc_url, 'origin': gov_name})
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
            if stop_flag:
                logger.info('到达上次爬取的链接')
                break
            start_record += 60
            end_record += 60
            url = second_url.format(start_record, end_record)
            response = request_site_page(url, params=data2, methods='post')
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
            page_num += 1
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
