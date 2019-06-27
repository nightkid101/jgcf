import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 内蒙古自治区税务局
# http://www.nmds.gov.cn/penaltylist
# http://www.nmds.gov.cn/nsfw/sscx/zdaj/
first_url = 'http://www.nmds.gov.cn/penaltylist'
second_url = 'http://www.nmds.gov.cn/nsfw/sscx/zdaj/ajxz/xkzzzyfp/'
gov_name = '内蒙古自治区税务局'
collection_name = 'tax_data'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
}
data = {
    'currentPageIndex': 1
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
    response = request_site_page(first_url, params=data, headers=headers, methods='post')
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(first_url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': first_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': first_url})['last_updated']
    else:
        last_updated_url = ''
    page_count = re.findall('(\d+)页  共\d+条', soup.text)[-1].strip()
    page_num = 1
    while page_num <= int(page_count):
        logger.info('第%d页' % page_num)
        try:
            data_list = soup.find(attrs={"id": "sgstable"}).find_all('tr')
            for index, each_data in enumerate(data_list):
                href = each_data.find('a')['href']
                anc_url = urljoin(first_url, href)

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
                publish_date = each_data.find(attrs={"class": "sgsgsrq"}).text.strip()
                title = each_data.find(attrs={"class": "sgsnrmc"}).text.strip()

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
            data['currentPageIndex'] = page_num
            response = request_site_page(first_url, params=data, headers=headers, methods='post')
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
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

    content = re.findall('document.write\(\'<tr><td([\s\S]*?)</td></tr>', soup.text)
    data_count = int(len(content) / 4)
    crawler_count = 0
    for index, each_data in enumerate(content):
        if (index + 1) > data_count * 4:
            break
        if int(index / 4) == crawler_count:
            href = re.search('<a href=\"([\s\S]*?)\" target=', each_data).group(1).strip()
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

            title = re.search('title=\"([\s\S]*?)\" style', each_data).group(1).strip()
            publish_date = re.search('(\d{8})', href).group(1).strip()
            publish_date = publish_date[0: 4] + '-' + publish_date[4: 6] + '-' + publish_date[6: 8]

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

            crawler_count += 1
        else:
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
