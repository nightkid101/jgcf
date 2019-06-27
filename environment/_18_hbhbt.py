import copy
import math
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 湖北省环境保护厅
# http://sthjt.hubei.gov.cn:8080/pub/root8/index.html?itemId=68
# http://sthjt.hubei.gov.cn/xxgk/xxgkml/hjjc/zdhjwf/lists.shtml
# http://sthjt.hubei.gov.cn/xxgk/xxgkml/hjjc/hjzfwj/lists.shtml
# http://sthjt.hubei.gov.cn/xxgk/xxgkml/hjjc/jcxx/lists.shtml
# http://sthjt.hubei.gov.cn/xxgk/xxgkml/hjjc/zxjk/lists.shtml
url_format_list = [
    'http://sthjt.hubei.gov.cn/xxgk/xxgkml/hjjc/zdhjwf/lists{}.shtml',
    'http://sthjt.hubei.gov.cn/xxgk/xxgkml/hjjc/hjzfwj/lists{}.shtml',
    'http://sthjt.hubei.gov.cn/xxgk/xxgkml/hjjc/jcxx/lists{}.shtml',
    'http://sthjt.hubei.gov.cn/xxgk/xxgkml/hjjc/zxjk/lists{}.shtml',
]
gov_name = '湖北省环境保护厅'
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


def get_gov_public():
    result_list = []
    url_format = 'http://sthjt.hubei.gov.cn:8080/pub/root8/16/68/nei{}.htm'
    response = request_site_page(url_format.format(''))
    logger.info("{} 抓取URL：{}".format(gov_name, url_format.format('')))
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url_format.format('')))
    result = re.search(r'm_nRecordCount = "(\d+)"', response.text if response else '')
    record_count = int(result.group(1)) if result else 0
    result = re.search(r'var m_nPageSize = (\d+)', response.text if response else '')
    page_size = int(result.group(1)) if result else 1
    page_count = math.ceil(record_count / page_size)
    logger.info('{} 一共有{}页'.format(gov_name, page_count))

    if db.crawler.find({'url': url_format.format('')}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': url_format.format('')})['last_updated']
    else:
        last_updated_url = ''

    for num in range(page_count):
        url = url_format.format('_' + str(num) if num != 0 else '')
        try:
            response = request_site_page(url)
            logger.info('第%d页' % (num + 1))
            if response is None:
                logger.error('网页请求错误{}'.format(url))
            soup = bs(response.content if response else '', 'lxml')
            for index, x in enumerate(soup.find_all(class_='row')):
                anc_url = x.find('a').attrs['href'].strip()
                if not anc_url.startswith('http'):
                    anc_url = urljoin(url_format.format(''), anc_url)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if num == 0 and index == 0:
                    if db.crawler.find({'url': url_format.format('')}).count() > 0:
                        if db.crawler.find_one({'url': url_format.format('')})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': url_format.format('')}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': url_format.format(''), 'last_updated': anc_url, 'origin': gov_name})

                if db[collection_name].count_documents({'url': anc_url}) == 0:
                    info = {
                        'title': x.find('a').text.strip(),
                        'publishDate': x.find(class_='fbrq').text,
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


def hb_crawler(url_format):
    result_list = []
    response = request_site_page(url_format.format(''))
    logger.info("{} 抓取URL：{}".format(gov_name, url_format.format('')))
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url_format.format('1')))
    result = re.search(r'createPageHTML\((\d+),', response.text if response else '')
    page_count = int(result.group(1)) if result else 0
    logger.info('{} 一共有{}页'.format(gov_name, page_count))

    if db.crawler.find({'url': url_format.format('')}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': url_format.format('')})['last_updated']
    else:
        last_updated_url = ''

    first_flag = True

    for num in range(page_count):
        url = url_format.format('_' + str(num) if num != 0 else '')
        logger.info('第%d页' % (num + 1))
        try:
            response = request_site_page(url)
            if response is None:
                logger.error('网页请求错误{}'.format(url))
            soup = bs(response.content if response else '', 'lxml')
            info = {
                'title': '',
                'publishDate': '',
                'url': '',
                'type': '行政处罚决定',
                'origin': gov_name,
                'status': 'not parsed'
            }
            for x in soup.find_all('td'):
                if x.p:
                    info['title'] = x.p.a.attrs['title'].strip()
                    anc_url = x.p.a.attrs['href'].strip()
                    if not anc_url.startswith('http'):
                        anc_url = urljoin(url_format.format(''), anc_url)
                    if anc_url == last_updated_url:
                        stop_flag = True
                        logger.info('到达上次爬取的链接')
                        break
                    if num == 0 and first_flag:
                        if db.crawler.find({'url': url_format.format('')}).count() > 0:
                            if db.crawler.find_one({'url': url_format.format('')})['last_updated'] != anc_url:
                                db.crawler.update_one({'url': url_format.format('')},
                                                      {'$set': {'last_updated': anc_url}})
                        else:
                            db.crawler.insert_one(
                                {'url': url_format.format(''), 'last_updated': anc_url, 'origin': gov_name})
                        first_flag = False
                    info['url'] = anc_url
                elif x.span:
                    info['publishDate'] = x.span.text
                    if db[collection_name].count_documents({'url': info['url']}) != 0:
                        continue
                    if config['crawler_update_type']['update_type'] == '0':
                        break
                    logger.info('{} 新公告：{} url: {}'.format(gov_name, info['title'], anc_url))
                    temp = copy.deepcopy(info)
                    if temp not in result_list:
                        result_list.append(temp)
            if stop_flag:
                logger.info('到达上次爬取的链接')
                break
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


def crawler():
    get_gov_public()
    for x in url_format_list:
        hb_crawler(x)


if __name__ == '__main__':
    crawler()
