import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 广西壮族自治区税务局
# http://www.gxgs.gov.cn/govinfo/zfxxgkml/sgs/
# http://www.gxgs.gov.cn/zdsswfajxxgbl/
first_url = 'http://www.gxgs.gov.cn/govinfo/zfxxgkml/sgs/index{}.html'
second_url = 'http://www.gxgs.gov.cn/search/search?channelid=250052&perpage=10&outlinepage=10&page={}'

gov_name = '广西壮族自治区税务局'
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
    url = first_url.format('')
    response = request_site_page(url)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': first_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': first_url})['last_updated']
    else:
        last_updated_url = ''
    page_num = 1
    while response.status_code != 404:
        try:
            data_list = soup.find(attrs={"class": "hotnews_list"}).find_all('li')
            for index, each_data in enumerate(data_list):
                title = each_data.find('a').text.strip()
                if re.search('(双公示|行政处罚)', title):
                    href = each_data.find('a')['href']
                    anc_url = urljoin(url, href)

                    if anc_url == last_updated_url:
                        stop_flag = True
                        logger.info('到达上次爬取的链接')
                        break
                    if index == 0 and page_num == 1:
                        if db.crawler.find({'url': first_url}).count() > 0:
                            if db.crawler.find_one({'url': first_url})['last_updated'] != anc_url:
                                db.crawler.update_one({'url': first_url}, {'$set': {'last_updated': anc_url}})
                        else:
                            db.crawler.insert_one(
                                {'url': first_url, 'last_updated': anc_url, 'origin': gov_name})

                    publish_date = each_data.find('span').text.replace('/', '')

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
            url = first_url.format('_' + str(page_num))
            page_num += 1
            response = request_site_page(url)
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    page_num = 1
    url = second_url.format(str(page_num))
    response = request_site_page(url)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': second_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': second_url})['last_updated']
    else:
        last_updated_url = ''
    if not re.search(r'暂无符合公布标准的重大税收违法案件信息。', soup.text):
        page_count_text = soup.find_all('a')[-1]['href']
        page_count = re.search('page=(\d+)', page_count_text).group(1).strip()
        while page_num <= int(page_count):
            try:
                data_list = soup.find(attrs={"class": "lb"}).find_all('dd')
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

                    if db[collection_name].count_documents({'url': anc_url}) == 0:
                        info = {
                            'title': title,
                            'publishDate': '',
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
                url = second_url.format(str(page_num))
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
