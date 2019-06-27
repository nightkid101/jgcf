import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 河北省税务局
# http://www.hebtax.gov.cn/hbsw/bsfw/xzsgs/xzcfxx/
# http://www.he-n-tax.gov.cn/hbswxxgk/gkml/index.html
first_url = 'http://www.hebtax.gov.cn/hbsw/bsfw/xzsgs/xzcfxx/index{}.html'
second_url = 'http://www.he-n-tax.gov.cn/hbswxxgk/gkml/1166/1267/1241/lists{}.html'
gov_name = '河北省税务局'
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
    response = request_site_page(first_url.format(''))
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(first_url.format('')))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': first_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': first_url})['last_updated']
    else:
        last_updated_url = ''
    page_count_text = soup.find(attrs={"class": "pagerji"}).text.strip()
    page_count = re.search('countPage = (\d+)', page_count_text).group(1).strip()
    page_num = 1
    url = first_url.format('')
    while page_num <= int(page_count):
        logger.info('第%d页' % page_num)
        try:
            info_list = soup.find(attrs={"class": "rgtbar_erjilist"}).find_all('li')
            for index, each_info in enumerate(info_list):
                href = each_info.find('a')['href']
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
                publish_date = re.search('\[(.*)\]', each_info.find('span').text.strip()).group(1).strip()
                title = each_info.find('a')['title']

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
    response = request_site_page(second_url.format(''))
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(second_url.format('')))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': second_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': second_url})['last_updated']
    else:
        last_updated_url = ''
    record_count = re.search('var m_nRecordCount = "(\d+)"', soup.head.text).group(1).strip()
    page_size = re.search('var m_nPageSize = (\d+)', soup.head.text).group(1).strip()
    page_count = int(int(record_count) / int(page_size)) + 1
    page_num = 1
    url = second_url.format('')
    while page_num < page_count:
        logger.info('第%d页' % page_num)
        try:
            data_list = soup.find_all('a')
            for index, each_data in enumerate(data_list):
                href = each_data['href']
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
                title = re.search('(.*)', each_data.text.strip()).group(1).strip()
                publish_date = each_data.find_all('li')[-3].text.strip()
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
            url = second_url.format('_' + str(page_num))
            page_num += 1
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
