import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 广东省税务局
# http://www.gd-n-tax.gov.cn/siteapps/webpage/gdtax/sgs/xzcf_list.jsp?sitecode=gdtax&webcode=gdsw
# http://www.gd-n-tax.gov.cn/gdsw/qsgg/common_tt.shtml # 需要输入图片验证码 todo
# http://www.gd-n-tax.gov.cn/siteapps/webpage/gdtax/zdsswfaj/index.jsp
first_url = 'http://www.gd-n-tax.gov.cn/siteapps/webpage/gdtax/sgs/xzcf_list.jsp'

third_url = 'http://www.gd-n-tax.gov.cn/siteapps/webpage/gdtax/zdsswfaj/query.jsp'

gov_name = '广东省税务局'
collection_name = 'tax_data'

channelId = [
    'b28dcb23c0a3496ca04ad1e39707d31e',
    'f4bcd55acb284eb7afaf8be4fd573cca',
    '030b623d1c094f9da5a94b6e9c66a578',
    'ee5a8c3bca414a64b5f04c96418edba6',
    '76e4455df97b4dd2b002f4be51e2f669',
    'adc6e4d228194537a2264e326e33099d',
    '39d05f421014430fafa894c9057e4053',
    '025e14dd98ce4b48824253d86e0c650e',
    '1e63950660364473894522d257ccf8f1',
    '908439689e1044b6a0ff8743a38c49c2',
    '779d14dafbfb497da46f83a5a101dc8f',
    '460c303ce3e94a68aac999ce41663485',
    'bdb7286237cf432db9d0e4673cf0e0cf',
    'b8487b88e8d54b2f9c3b30a81914c14f',
    'd939f9d648bb43f2b654e92c60b63415',
    'f8890b0c70c342ce8d4bb001ceb165b1',
    'b3651ebfd3cd411b804b1e745afaa431',
    'a1359dd09b1a48fdbc30523b4aa47ea6',
    '94db0e948b0c46a09255b0b216d1dc5f',
    '06994b8b6d4d4215bd695962cf44baae',
    '4c729fe87066483fbce6d0e3b7a47c14'
]
params = {
    'dfbm': '440100JG',
    'webcode': 'gdsw',
    'sitecode': 'gdtax',
    'curPageNo': 1
}
params_3 = {
    'pageSize': 10,
    'pageNo': 1,
    'channelId': channelId[0]
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
    response = request_site_page(first_url, methods='post', params=params)
    response.encoding = response.apparent_encoding
    if response is None:
        logger.error('网页请求错误{}'.format(first_url))
    soup = bs(response.content if response else '', 'lxml')
    # 先获取所有城市对应的id
    dfbm_list = []
    city_data = soup.find_all(attrs={"class": "li-level1"})
    for each_data in city_data:
        all_area = each_data.find_all('h4')
        for each_area in all_area:
            dfbm_list.append(each_area.find('span')['id'])
    for each_city in dfbm_list:
        stop_flag = False
        if db.crawler.find({'url': each_city + first_url}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': each_city + first_url})['last_updated']
        else:
            last_updated_url = ''
        params['curPageNo'] = 1
        params['dfbm'] = each_city
        response = request_site_page(first_url, methods='post', params=params)
        response.encoding = response.apparent_encoding
        soup = bs(response.content if response else '', 'lxml')
        page_count_text = soup.find(attrs={"class": "pagediv clearfix"}).text.strip()
        page_count = re.search('共\s*(\d+)\s*页', page_count_text).group(1).strip()
        page_num = 1
        while page_num <= int(page_count):
            logger.info('抓取%s地区, 第%d页' % (each_city, page_num))
            try:
                data_list = soup.find(attrs={"class": "xx_article"}).find_all('li')
                for index, each_data in enumerate(data_list):
                    href = each_data.find('a')['href']
                    anc_url = urljoin(first_url, href)

                    if anc_url == last_updated_url:
                        stop_flag = True
                        logger.info('到达上次爬取的链接')
                        break
                    if db.crawler.find({'url': each_city + first_url}).count() > 0:
                        if db.crawler.find_one({'url': each_city + first_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': each_city + first_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': each_city + first_url, 'last_updated': anc_url, 'origin': gov_name})
                    title = each_data.find('a')['title']
                    publish_date = each_data.find(attrs={"class": "xx_article_date"}).text

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
                params['curPageNo'] = page_num
                response = request_site_page(first_url, methods='post', params=params)
                response.encoding = response.apparent_encoding
                soup = bs(response.content if response else '', 'lxml')
            except Exception as e:
                logger.error(e)
                logger.warning('提取公告url出现问题')
                continue
    for each_city in channelId:
        params_3['channelId'] = each_city
        params_3['pageNo'] = 1
        page_num = 1
        response = request_site_page(third_url, methods='post', params=params_3)
        response.encoding = response.apparent_encoding
        stop_flag = False
        if response is None:
            logger.error('网页请求错误{}'.format(third_url))
        soup = bs(response.content if response else '', 'lxml')

        if db.crawler.find({'url': each_city+third_url}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': each_city+third_url})['last_updated']
        else:
            last_updated_url = ''
        page_count = re.search('共\s*(\d+)\s*页', soup.text).group(1).strip()
        base_url = 'http://www.gd-n-tax.gov.cn/siteapps/webpage/gdtax/zdsswfaj/service.jsp?manuscriptId='
        while page_num <= int(page_count):
            logger.info('抓取%s地区, 第%d页' % (each_city, page_num))
            try:
                data_list = soup.find(attrs={"class": "select-list"}).find_all('li')
                for index, each_data in enumerate(data_list):
                    article_id = re.search('queryIllegalDetail\(\'(.*)\'\)', each_data.find('a')['onclick']).group(
                        1).strip()
                    anc_url = base_url + article_id

                    if anc_url == last_updated_url:
                        stop_flag = True
                        logger.info('到达上次爬取的链接')
                        break
                    if index == 0 and page_num == 1:
                        if db.crawler.find({'url': each_city+third_url}).count() > 0:
                            if db.crawler.find_one({'url': each_city+third_url})['last_updated'] != anc_url:
                                db.crawler.update_one({'url': each_city+third_url}, {'$set': {'last_updated': anc_url}})
                        else:
                            db.crawler.insert_one(
                                {'url': each_city+third_url, 'last_updated': anc_url, 'origin': gov_name})
                    title = each_data.find('a').text.strip()
                    publish_date = each_data.find('font').text.replace('[', '').replace(']', '')

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
                params_3['pageNo'] = page_num
                response = request_site_page(third_url, methods='post', params=params_3)
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
