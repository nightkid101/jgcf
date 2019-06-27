import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 河南省税务局
# http://henan.chinatax.gov.cn/003/xxgk_301/xzxk_30110/hzxkhzcfgs_3011002/3011002_list_0.html?NVG=1&LM_ID=3011002
# http://henan.chinatax.gov.cn/003/trans.html?url=/websites/comm/zfxxgkml/zfxxgkml.html&NVG=1&LM_ID=3010803
# http://henan.chinatax.gov.cn/zxhd/websites/comm/zdsswfxxgb/xxk_index.html?SWJG_DM=14100140000&NVG=1&LM_ID=30116
# http://henan.chinatax.gov.cn/003/trans.html?url=/websites/comm/zfxxgkml/zfxxgkml.html&NVG=1&LM_ID=3010803
first_url = 'http://henan.chinatax.gov.cn/003/xxgk_301/xzxk_30110/hzxkhzcfgs_3011002/3011002_list_{}.html' \
            '?NVG=1&LM_ID=3011002'
second_url = 'http://henan.chinatax.gov.cn/zxhd/cms/wd/getWD_PATHforZFXXGKML.do'
third_url = 'http://henan.chinatax.gov.cn/zxhd/base/wfaj/getWFAJ_NSR_HTML.do'
forth_url = 'http://henan.chinatax.gov.cn/zxhd/cms/wd/getWD_PATHforZFXXGKML.do'

gov_name = '河南省税务局'
collection_name = 'tax_data'

params = {
    'WZ_ID': '003',
    'CURPAGE': 1,
    'PAGESIZE': 11,
    'LM_ID': 3011002,
    'WD_GKLX': 1201,
    'WD_ZT': 1,
    'CUR_USERID': 'null'
}
params_3 = {
    'CURPAGE': 2,
    'PAGESIZE': 10
}
params_4 = {
    'WZ_ID': '003',
    'CURPAGE': 1,
    'PAGESIZE': 11,
    'LM_ID': 3011501,
    'WD_GKLX': 1201,
    'WD_ZT': 1,
    'CUR_USERID': 'null'
}
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
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
    url = first_url.format('0')
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
    page_count_text = soup.find(attrs={"class": "sabrosus"}).find_all('a')[-1]['href']
    page_count = re.search('_list_(\d+)', page_count_text).group(1).strip()
    page_num = 0
    while page_num <= int(page_count):
        try:
            data_list = soup.find(attrs={"class": "info_list"}).find_all('li')
            for index, each_data in enumerate(data_list):
                href = each_data.find('a')['href']
                anc_url = urljoin(url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0 and page_num == 0:
                    if db.crawler.find({'url': first_url}).count() > 0:
                        if db.crawler.find_one({'url': first_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': first_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': first_url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_data.find('a').text.strip()
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
            url = first_url.format(page_num)
            response = request_site_page(url)
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    page_num = 1
    response = request_site_page(second_url, methods='post', params=params)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(second_url))

    if db.crawler.find({'url': second_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': second_url})['last_updated']
    else:
        last_updated_url = ''
    total_records = response.json()['mapParam']['TOTAL']
    per_page_records = response.json()['mapParam']['PAGESIZE']
    page_count = int(int(total_records) / int(per_page_records)) + 1
    while page_num <= page_count:
        try:
            data_list = response.json()['data']
            for index, each_data in enumerate(data_list):
                href = each_data['wdPath']
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
                title = each_data['WD_BT']
                publish_date = each_data['WD_CJRQ']

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
            params['CURPAGE'] = page_num
            response = request_site_page(second_url, methods='post', params=params)
            response.encoding = response.apparent_encoding
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    page_num = 1
    params_3['CURPAGE'] = page_num
    response = request_site_page(third_url, methods='post', params=params_3, headers=headers)
    response.encoding = response.apparent_encoding
    soup = bs(response.content if response else '', 'lxml')
    page_count_text = soup.body.find_all('table')[2].find_all('tr')[-1].text
    page_count = re.search('\d+/(\d+)', page_count_text).group(1).strip()
    stop_flag = False
    if db.crawler.find({'url': third_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': third_url})['last_updated']
    else:
        last_updated_url = ''
    while page_num <= int(page_count):
        try:
            data_list = soup.body.find_all('table')[2].find_all('tr')
            for index, each_data in enumerate(data_list):
                if re.search('页面大小', each_data.text):
                    break
                href = each_data.find('a')['href']
                anc_url = urljoin(url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0 and page_num == 1:
                    if db.crawler.find({'url': third_url}).count() > 0:
                        if db.crawler.find_one({'url': third_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': third_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one({'url': third_url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_data.find('a').text.strip()
                publish_date_raw = re.search('(\d{8})', href).group(1).strip()
                publish_date = publish_date_raw[0: 4] + '-' + publish_date_raw[4: 6] + '-' + publish_date_raw[6: 8]

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
            params_3['CURPAGE'] = page_num
            response = request_site_page(third_url, methods='post', params=params, headers=headers)
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    response = request_site_page(forth_url, methods='post', params=params_4)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if db.crawler.find({'url': forth_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': forth_url})['last_updated']
    else:
        last_updated_url = ''
    try:
        data_list = response.json()['data']
        for index, each_data in enumerate(data_list):
            href = each_data['wdPath']
            anc_url = urljoin(url, href)

            if anc_url == last_updated_url:
                stop_flag = True
                logger.info('到达上次爬取的链接')
                break
            if index == 0:
                if db.crawler.find({'url': forth_url}).count() > 0:
                    if db.crawler.find_one({'url': forth_url})['last_updated'] != anc_url:
                        db.crawler.update_one({'url': forth_url}, {'$set': {'last_updated': anc_url}})
                else:
                    db.crawler.insert_one({'url': forth_url, 'last_updated': anc_url, 'origin': gov_name})
            title = each_data['WD_BT']
            publish_date = each_data['WD_CJRQ']

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
    except Exception as e:
        logger.error(e)
        logger.warning('提取公告url出现问题')
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
