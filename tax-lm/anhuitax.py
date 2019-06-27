import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 安徽省税务局
# http://www.creditah.gov.cn/DepartPublicity/index.htm?dp=15&type=2
# http://anhui.chinatax.gov.cn/col/col7488/index.html
# http://anhui.chinatax.gov.cn/col/col5559/index.html
# http://credit.ah.gov.cn/AdministrativePenalty/index.htm?dp=16
first_url = 'http://credit.ah.gov.cn/DepartPublicity/index_{}.htm?dp=15&type=2'
second_url = 'http://www.ah-n-tax.gov.cn/module/newinterface/bulletin/ajaxdata.jsp'
third_url = 'http://anhui.chinatax.gov.cn/col/col5559/index.html'
forth_url = 'http://credit.ah.gov.cn/AdministrativePenalty/index_{}.htm?dp=16'
gov_name = '安徽省税务局'
collection_name = 'tax_data'

area_list = [
    '黄山市',
    '马鞍山市',
    '阜阳市',
    '阜阳市',
    '蚌埠市',
    '芜湖市',
    '滁州市',
    '淮南市',
    '淮北市',
    '池州市',
    '宣城市',
    '安庆市',
    '合肥市',
    '六安市'
]
params = {
    'startrecord': 1,
    'endrecord': 20,
    'perpage': 15,
    'searhvalue': area_list[0],
    'searchkey': 'area',
    'year': ''
}

item_list = ['行政处罚']

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

    if db.crawler.find({'url': first_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': first_url})['last_updated']
    else:
        last_updated_url = ''
    page_count = re.findall('共(\d+)页', soup.text)[-1]
    while page_num <= int(page_count):
        try:
            data_list = soup.find(attrs={"class": "publicity_table"}).find_all('tr')
            for index, each_data in enumerate(data_list):
                href = each_data.find('a')['href']
                anc_url = urljoin(url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break

                if page_num == 1 and index == 0:
                    if db.crawler.find({'url': first_url}).count() > 0:
                        if db.crawler.find_one({'url': first_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': first_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': first_url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_data.find('a')['title'].strip()
                publish_date = each_data.find_all('td')[-1].text.strip()

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
            page_num += 1
            url = first_url.format(str(page_num))
            response = request_site_page(url)
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
            if stop_flag:
                logger.info('到达上次爬取的链接')
                break
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    response = request_site_page(second_url, params=params, methods='post')
    response.encoding = response.apparent_encoding
    if response is None:
        logger.error('网页请求错误{}'.format(second_url))

    for each_area in area_list:
        stop_flag = False
        if db.crawler.find({'url': each_area + ' ' + second_url}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': each_area + ' ' + second_url})['last_updated']
        else:
            last_updated_url = ''
        try:
            params['searhvalue'] = each_area
            response = request_site_page(second_url, params=params, methods='post')
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
            data_list = soup.find_all('tr')
            for index, each_data in enumerate(data_list):
                href = each_data.find('a')['href']
                anc_url = urljoin(second_url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0:
                    if db.crawler.find({'url': each_area + ' ' + second_url}).count() > 0:
                        if db.crawler.find_one({'url': each_area + ' ' + second_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': each_area + ' ' + second_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': each_area + ' ' + second_url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_data.find('a').text.strip()
                publish_date = re.search('(\d+/\d+/\d+)', href).group(1).strip().replace('/', '-')

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
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    response = request_site_page(third_url)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(third_url))

    XZCF_url = 'http://anhui.chinatax.gov.cn/module/xxgk/subjectinfo.jsp?showsub=1'
    data_list = [{
        'infotypeId': 0,
        'jdid': '39',
        'nZtflid': 40,
        'vc_bm': '0702',
        'area': '11340000002986061N',
        'strSearchUrl': '/module/xxgk/subjectinfo.jsp'}]

    for item in item_list:
        if db.crawler.find({'url': item + third_url}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': item + third_url})['last_updated']
        else:
            last_updated_url = ''

        response = request_site_page(XZCF_url, params=data_list[0], methods='post')
        response.encoding = response.apparent_encoding
        soup = bs(response.text, 'lxml')
        tr_list = soup.find('table', style = 'border-top:none').find_all('tr')
        del(tr_list[0])
        for index, each_tr in enumerate(tr_list):
            try:
                href = each_tr.find('a')['href']
                anc_url = urljoin(third_url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0:
                    if db.crawler.find({'url': item + third_url}).count() > 0:
                        if db.crawler.find_one({'url': item + third_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': item + third_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one({'url': item + third_url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_tr.find('a').text.strip()
                publish_date = each_tr.find('td', align='center').text.strip()

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
            except Exception as e:
                logger.error(e)
                logger.warning('提取公告url出现问题')

    url = forth_url.format('1')
    response = request_site_page(url)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': forth_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': forth_url})['last_updated']
    else:
        last_updated_url = ''
    page_count = re.findall('共(\d+)页', soup.text)[-1]
    page_num = 1
    while page_num <= int(page_count):
        try:
            data_list = soup.find(attrs={"class": "publicity_table"}).find_all('tr')
            for index, each_data in enumerate(data_list):
                href = each_data.find_all('a')[-1]['href']
                anc_url = urljoin(url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0 and page_num == 1:
                    if db.crawler.find({'url': forth_url}).count() > 0:
                        if db.crawler.find_one({'url': forth_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': forth_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one({'url': forth_url, 'last_updated': anc_url, 'origin': gov_name})
                title = each_data.find_all('a')[-1]['title']
                publish_date = each_data.find_all('td')[-1].text.strip()

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
            page_num += 1
            url = forth_url.format(str(page_num))
            response = request_site_page(url)
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
            if stop_flag:
                logger.info('到达上次爬取的链接')
                break
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            if not each_data.find('a'):
                logger.info('公告已提取完毕')
                break
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
