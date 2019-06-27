import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

from selenium import webdriver

chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1440,900')
chrome_options.add_argument('--silent')
chrome_options.add_argument('lang=zh_CN.UTF-8')
chrome_options.add_argument('Accept-Encoding="gzip, deflate"')
chrome_options.add_argument('Accept-Language="zh-CN,zh;q=0.9,en;q=0.8"')
chrome_options.add_argument('Cache-Control="no-store"')
chrome_options.add_argument('Connection="keep-alive"')
chrome_options.add_argument('Upgrade-Insecure-Requests="1"')
chrome_options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"')

from time import sleep

# 江苏省税务局
# http://www.jsgs.gov.cn/col/col5514/index.html
# http://www.jsgs.gov.cn/col/col7223/index.html
# http://www.jsgs.gov.cn/col/col7277/index.html
first_url = 'https://etax.jsgs.gov.cn/portal/queryapi/commonPage.do?sign=query_ggcx_xkqz'
second_url = 'http://jiangsu.chinatax.gov.cn/col/col7223/index.html'
third_url = 'http://www.jsgs.gov.cn/col/col7277/index.html'
gov_name = '江苏省税务局'
collection_name = 'tax_data'

params = {
    'startrecord': 1,
    'endrecord': 60,
    'perpage': 20,
    'col': 1,
    'appid': 1,
    'webid': 18,
    'path': '/',
    'columnid': 7223,
    'sourceContentTyp': 1,
    'unitid': 21948,
    'webname': '国家税务总局江苏省税务局网站',
    'permissiontype': 0
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
    # response = request_site_page(first_url)
    # response.encoding = response.apparent_encoding
    # if response is None:
    #     logger.error('网页请求错误{}'.format(first_url))
    # soup = bs(response.content if response else '', 'lxml')
    # json_list = json.loads(re.search('var cityList =([\s\S]*?);', soup.text).group(1).strip())[0]
    # area_id = []
    # for each_city_code in json_list:
    #     area_list = json_list[each_city_code]
    #     for each_area in area_list:
    #         area_id.append(each_area['xzqh_dm'])
    # base_url = 'https://etax.jiangsu.chinatax.gov.cn/portal/queryapi/queryPage.do?sign=queryXzxk&xzlb=1&orgid='
    # for each_area in area_id:
    #     try:
    #         url = base_url + str(each_area)
    #         logger.info('url to crawler:' + url)
    #         response = request_site_page(url)
    #         response.encoding = response.apparent_encoding
    #         soup = bs(response.content if response else '', 'lxml')
    #
    #         if db.crawler.find({'url': each_area + first_url}).count() > 0:
    #             last_updated_content = db.crawler.find_one({'url': each_area + first_url})['last_updated']
    #         else:
    #             last_updated_content = ''
    #
    #         page_count_text = soup.find(attrs={"class": "clear pages"}).text.strip()
    #         page_count = re.search('共 (\d+) 页', page_count_text).group(1).strip()
    #         logger.info('共%s页' % page_count)
    #         page_num = 1
    #         stop_flag = False
    #         logger.info('第%d页' % page_num)
    #         # 第一页抓取方法
    #         data_list = soup.find(attrs={"id": "pagelist_ul"}).find_all('li')
    #         for index, each_data in enumerate(data_list):
    #             title = each_data.find('a')['title']
    #             publish_date = each_data.find(attrs={"class": "xx_article_date"}).text.strip()
    #             announcement_code = each_data.find(attrs={"name": "xk_wsh"})['value'].strip()
    #             content_text = each_data.find(attrs={"name": "xk_nr"})['value'].strip()
    #             litigant = each_data.find(attrs={"name": "xk_nsrmc"})['value'].strip()
    #
    #             if content_text == last_updated_content:
    #                 stop_flag = True
    #                 logger.info('到达上次爬取的链接')
    #                 break
    #             if index == 0 and page_num == 1:
    #                 if db.crawler.find({'url': each_area + first_url}).count() > 0:
    #                     if db.crawler.find_one({'url': each_area + first_url})['last_updated'] != content_text:
    #                         db.crawler.update_one({'url': each_area + first_url}, {'$set': {'last_updated': content_text}})
    #                 else:
    #                     db.crawler.insert_one(
    #                         {'url': each_area + first_url, 'last_updated': content_text, 'origin': gov_name})
    #
    #             info = {
    #                 'title': title,
    #                 'publishDate': publish_date,
    #                 'announcementCode': announcement_code,
    #                 'contentText': content_text,
    #                 'litigant': litigant,
    #                 'type': '行政处罚决定',
    #                 'origin': gov_name,
    #                 'status': 'not parsed'
    #             }
    #             if info not in result_list:
    #                 result_list.append(info)
    #         if stop_flag:
    #             logger.info('到达上次爬取的链接')
    #             continue
    #         page_num += 1
    #         # 第二页以后的抓取方法
    #         while page_num <= int(page_count):
    #             logger.info('第%d页' % page_num)
    #             more_page_url = 'https://etax.jiangsu.chinatax.gov.cn/portal/queryapi/queryPageList.do?page=' \
    #                             + str(page_num) + '&rows=10&orgid=' + str(each_area) + '&xzlb=1'
    #             sub_response = request_site_page(more_page_url)
    #             sub_response.encoding = sub_response.apparent_encoding
    #             soup = bs(sub_response.content if sub_response else '', 'lxml')
    #             data_list = json.loads(soup.text)['DATA']['list']
    #             for each_data in data_list:
    #                 title = each_data['xk_xmmc']
    #                 publish_date = each_data['sjc']
    #                 announcement_code = each_data['xk_wsh']
    #                 content_text = each_data['xk_nr']
    #                 litigant = each_data['xk_nsrmc']
    #
    #                 if content_text == last_updated_content:
    #                     stop_flag = True
    #                     logger.info('到达上次爬取的链接')
    #                     break
    #
    #                 info = {
    #                     'title': title,
    #                     'publishDate': publish_date,
    #                     'announcementCode': announcement_code,
    #                     'contentText': content_text,
    #                     'litigant': litigant,
    #                     'type': '行政处罚决定',
    #                     'origin': gov_name,
    #                     'status': 'not parsed'
    #                 }
    #                 if info not in result_list:
    #                     result_list.append(info)
    #             if stop_flag:
    #                 logger.info('到达上次爬取的链接')
    #                 break
    #             page_num += 1
    #     except Exception as e:
    #         logger.error(e)
    #         logger.warning('提取公告url出现问题')
    #         continue
    browser = webdriver.Chrome(chrome_options=chrome_options)
    browser.set_page_load_timeout(50)
    browser.get(second_url)
    browser.implicitly_wait(5)
    response = browser.page_source
    # response = request_site_page('http://jiangsu.chinatax.gov.cn/index.html')
    # response = request_site_page(second_url)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(second_url))
    soup = bs(response.content if response else '', 'lxml')

    if db.crawler.find({'url': second_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': second_url})['last_updated']
    else:
        last_updated_url = ''
    page_count = soup.totalpage.text.strip()
    page_count = int(int(page_count) / 3)
    page_num = 0

    while page_num <= page_count:
        try:
            response = request_site_page(second_url, methods='post', params=params)
            response.encoding = response.apparent_encoding
            soup = bs(response.content if response else '', 'lxml')
            data_list = soup.find_all('record')
            params['startrecord'] += 60
            params['endrecord'] += 60
            for index, each_data in enumerate(data_list):
                href = each_data.find('a')['href']
                anc_url = urljoin(second_url, href)

                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0 and page_num == 0:
                    if db.crawler.find({'url': second_url}).count() > 0:
                        if db.crawler.find_one({'url': second_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': second_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': second_url, 'last_updated': anc_url, 'origin': gov_name})
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
        except Exception as e:
            logger.error(e)
            logger.warning('提取公告url出现问题')
            continue
    # response = request_site_page(third_url)
    # response.encoding = response.apparent_encoding
    # if response is None:
    #     logger.error('网页请求错误{}'.format(third_url))
    # soup = bs(response.content if response else '', 'lxml')
    # data_list = re.findall('<record><!\[CDATA\[([\s\S]*?)</tr>]]></record>', soup.text)
    # for each_data in data_list:
    #     href = re.search('<a href=\'([\s\S]*?)\' target=', each_data).group(1).strip()
    #     anc_url = urljoin(third_url, href)
    #     title = re.search('class=\"tdbgcl\">([\s\S]*?)</a></td>', each_data).group(1).strip()
    #     publish_date = re.search('(\d+/\d+/\d+)', each_data).group(1).strip().replace('/', '-')
    #
    #     info = {
    #         'title': title,
    #         'publishDate': publish_date,
    #         'url': anc_url,
    #         'type': '行政处罚决定',
    #         'origin': gov_name,
    #         'status': 'not parsed'
    #     }
    #     logger.info('{} 新公告：{} url: {}'.format(gov_name, info['title'], anc_url))
    #     if info not in result_list:
    #         result_list.append(info)
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
