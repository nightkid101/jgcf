import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 重庆市环境保护局
# http://sthjj.cq.gov.cn/hjgl/xzcf/xzcfjd/index.shtml
# http://sthjj.cq.gov.cn/hjgl/xzcf/hjwfxwzlgzjd/index.shtml
# http://sthjj.cq.gov.cn/hjgl/xzcf/sqfyqzzxmd/index.shtml
# http://sthjj.cq.gov.cn/xxgk/zfxxgkml/zcfg/xzfy/index.shtml
# http://sthjj.cq.gov.cn/xxgk/zfxxgkml/zcfg/xzss/index.shtml
# http://sthjj.cq.gov.cn/hjgl/hjzf/gpdb/index.shtml
# http://sthjj.cq.gov.cn/hjgl/hjzf/cfkyzcwrpfdsssbq/index.shtml
url_format_list_first = [
    'http://sthjj.cq.gov.cn/hjgl/xzcf/xzcfjd/index{}.shtml',
    'http://sthjj.cq.gov.cn/hjgl/xzcf/hjwfxwzlgzjd/index{}.shtml',
    'http://sthjj.cq.gov.cn/hjgl/xzcf/sqfyqzzxmd/index{}.shtml',
    'http://sthjj.cq.gov.cn/hjgl/hjzf/gpdb/index{}.shtml',
    'http://sthjj.cq.gov.cn/hjgl/hjzf/cfkyzcwrpfdsssbq/index{}.shtml'
]
url_format_list_second = [
    'http://sthjj.cq.gov.cn/xxgk/zfxxgkml/zcfg/xzfy/index.shtml',
    'http://sthjj.cq.gov.cn/xxgk/zfxxgkml/zcfg/xzss/index.shtml'
]
gov_name = '重庆市环境保护局'
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


def cq_crawler_first(url_format):
    result_list = []
    response = request_site_page(url_format.format(''))
    logger.info("{} 抓取URL：{}".format(gov_name, url_format.format('')))
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url_format.format('')))
    soup = bs(response.content if response else '', 'lxml')
    page_count_text = soup.body.find(class_='fenye')
    page_count = int(re.findall(r'\d+', str(page_count_text.text) if page_count_text else '')[1])
    logger.info('{} 一共有{}页'.format(gov_name, page_count))

    if db.crawler.find({'url': url_format.format('')}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': url_format.format('')})['last_updated']
    else:
        last_updated_url = ''

    for num in range(page_count):
        url = url_format.format('_' + str(num + 1) if num != 0 else '')
        try:
            response = request_site_page(url)
            logger.info('第%d页' % (num + 1))
            if response is None:
                logger.error('网页请求错误{}'.format(url))
            soup = bs(response.content if response else '', 'lxml')
            for index, x in enumerate(soup.body.find(class_='list_main_right_content').find_all('li')):
                anc_url = x.a.attrs['href'].strip()
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
                        'title': x.a.text,
                        'publishDate': x.span.text,
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


def cq_crawler_second(url_format):
    result_list = []
    response = request_site_page(url_format.format(''))
    logger.info("{} 抓取URL：{}".format(gov_name, url_format.format('')))
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url_format.format('')))
    soup = bs(response.content if response else '', 'lxml')
    page_count_text = soup.body.find(id='page')
    page_count = int(re.findall(r'\d+', str(page_count_text.text) if page_count_text else '')[1])
    logger.info('{} 一共有{}页'.format(gov_name, page_count))

    if db.crawler.find({'url': url_format.format('')}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': url_format.format('')})['last_updated']
    else:
        last_updated_url = ''

    for num in range(page_count):
        url = url_format.format('_' + str(num + 1) if num != 0 else '')
        try:
            response = request_site_page(url)
            logger.info('第%d页' % (num + 1))
            if response is None:
                logger.error('网页请求错误{}'.format(url))
            soup = bs(response.content if response else '', 'lxml')
            for index, x in enumerate(soup.body.find(class_='tableCont').find_all('li')):
                anc_url = x.a.attrs['href'].strip()
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
                        'title': x.a.text,
                        'publishDate': x.span.text,
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


def crawler():
    for x in url_format_list_first:
        cq_crawler_first(x)
    for x in url_format_list_second:
        cq_crawler_second(x)


if __name__ == '__main__':
    crawler()
