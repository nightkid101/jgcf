import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 福建省税务局
# http://fujian.chinatax.gov.cn/zfxxgkzl/zfxxgkml/xzzf/zdsswfajgbl/
# http://wssw.fjtax.gov.cn/etax/135/sscx/xzcfxyInfo.jsp     # 暂无数据
first_url = 'http://fujian.chinatax.gov.cn/was5/web/search?channelid=203958&sortfield=-pubdate' \
      '&classsql=docpuburl%3D%27%25http%3A%2F%2Fwww.fj-n-tax.gov.cn%2Fzfxxgkzl%2Fzfxxgkml%2Fxzzf%2Fzdsswfajgbl%2F%25%27' \
      '&random=0.7884730470356949&prepage=10&page={}'
gov_name = '福建省税务局'
collection_name = 'tax_data'

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
    page_num = 1
    url = first_url.format(str(page_num))
    response = request_site_page(url)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': url})['last_updated']
    else:
        last_updated_url = ''
    json_text = soup.p.text.strip().replace('\'', '\"')
    json_data = json.loads(json_text, strict=False)
    page_count = json_data['pagenum']
    while page_num <= int(page_count):
        try:
            data_list = json_data['docs']
            for index, each_data in enumerate(data_list):
                anc_url = each_data['url']

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0 and page_num == 1:
                    if db.crawler.find({'url': url}).count() > 0:
                        if db.crawler.find_one({'url': url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_data['title']
                publish_date = each_data['time']

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
            url = first_url.format(str(page_num))
            response = request_site_page(url)
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
            json_text = soup.p.text.strip().replace('\'', '\"')
            json_data = json.loads(json_text, strict=False)
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
