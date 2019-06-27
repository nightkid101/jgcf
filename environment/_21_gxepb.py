import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 广西壮族自治区环境保护厅
# http://sthjt.gxzf.gov.cn/ztzl/lsztzl/hjwfbgt/
# http://sthjt.gxzf.gov.cn/sgs/xzcfList.jsp?type=cf
# http://sthjt.gxzf.gov.cn/xxgkml/ztfl/hjglywxx/xzcfhfy/

url_format_list = [
    'http://sthjt.gxzf.gov.cn/xxgkml/ztfl/hjglywxx/xzcfhfy/index{}.html'
]
url_format_list2 = [
    'http://sthjt.gxzf.gov.cn/ztzl/lsztzl/hjwfbgt/index{}.html',
]
url_format_list3 = [
    {'get_url': 'http://sthjt.gxzf.gov.cn/sgs/xzcfList.jsp?type=cf',
     'post_url': 'http://sthjt.gxzf.gov.cn/sgs/middle_cf.jsp'}
]
gov_name = '广西壮族自治区环境保护厅'
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


def gx_crawler(url_format):
    result_list = []
    response = request_site_page(url_format.format(''))
    logger.info("{} 抓取URL：{}".format(gov_name, url_format.format('')))
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url_format.format('')))
    soup = bs(response.content if response else '', 'html5lib')
    page_count_text = soup.body.find(class_='pages')
    page_count = int(re.findall(r'createPageHTML\((\d+)', page_count_text.text if page_count_text else '')[-1])
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
            soup = bs(response.content if response else '', 'html5lib')
            for index, x in enumerate(soup.find(class_='list-mod-bd').find_all('li')):
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
                        'title': x.a.text.strip(),
                        'publishDate': x.i.text.strip(),
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


def gx_crawler2(url_format):
    result_list = []
    response = request_site_page(url_format.format(''))
    logger.info("{} 抓取URL：{}".format(gov_name, url_format.format('')))
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url_format.format('')))
    soup = bs(response.content if response else '', 'html5lib')
    page_count_text = soup.body.find(class_='pages')
    page_count = int(re.findall(r'createPageHTML\((\d+)', page_count_text.text if page_count_text else '')[-1])
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
            soup = bs(response.content if response else '', 'html5lib')
            for index, x in enumerate(soup.find(class_='news-list').find_all('li')):
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
                        'title': re.sub(r'\d{4}-\d{1,2}-\d{1,2}$', '', x.a.text.strip()),
                        'publishDate': x.span.text.strip(),
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


def gx_crawler3(url_format):
    result_list = []
    data = {
        'FROM_SELF': 'true',
        'q_WH': '',
        'q_XDR': '',
        'P_PAGES': '',
        'P_RECORD_COUNT': '',
        'P_PAGESIZE': 15,
        'P_CURRENT': 1,
        'P_PAGE_ORDER_WAY': 'desc'
    }
    response = request_site_page(url_format['post_url'], methods='post', data=data)
    logger.info("{} 抓取URL：{}".format(gov_name, url_format['get_url']))
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url_format['get_url']))
    soup = bs(response.content if response else '', 'html5lib')
    page_count = int(soup.find(id='P_PAGES').attrs['value'])
    announcement_count = int(soup.find(id='P_RECORD_COUNT').attrs['value'])
    logger.info('{} 一共有{}页'.format(gov_name, page_count))

    if db.crawler.find({'url': url_format['get_url']}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': url_format['get_url']})['last_updated']
    else:
        last_updated_url = ''

    for num in range(page_count):
        data = {
            'FROM_SELF': 'true',
            'q_WH': '',
            'q_XDR': '',
            'P_PAGES': page_count,
            'P_RECORD_COUNT': announcement_count,
            'P_PAGESIZE': 15,
            'P_CURRENT': num+1,
            'P_PAGE_ORDER_WAY': 'desc'
        }
        try:
            response = request_site_page(url_format['post_url'], methods='post', data=data)
            logger.info('第%d页' % (num + 1))
            if response is None:
                logger.error('网页请求错误第%d页' % (num + 1))
            soup = bs(response.content if response else '', 'html5lib')
            for index, x in enumerate(soup.find(id='list_table_1').find_all('tr')[1:]):
                anc_url = x.a.attrs['href'].strip()
                if not anc_url.startswith('http'):
                    anc_url = urljoin(url_format['get_url'], anc_url)
                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if num == 0 and index == 0:
                    if db.crawler.find({'url': url_format['get_url']}).count() > 0:
                        if db.crawler.find_one({'url': url_format['get_url']})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': url_format['get_url']}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': url_format['get_url'], 'last_updated': anc_url, 'origin': gov_name})

                if db[collection_name].count_documents({'url': anc_url}) == 0:
                    info = {
                        'title': x.find_all('td')[1].text.strip(),
                        'publishDate': x.find_all('td')[3].text.strip(),
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
    for x in url_format_list:
        gx_crawler(x)
    for x in url_format_list2:
        gx_crawler2(x)
    for x in url_format_list3:
        gx_crawler3(x)


if __name__ == '__main__':
    crawler()
