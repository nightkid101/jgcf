import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 湖南省税务局
# http://hunan.chinatax.gov.cn/zhuanti/ajpgt/
# http://hunan.chinatax.gov.cn/article_listcf.jsp?smallclassid=20160216225002&includecity=1&cityvisible=1&idused=0
# http://hunan.chinatax.gov.cn/zhuanti/qsgg/
# http://hunan.chinatax.gov.cn/gkml.jsp
first_url = 'http://hunan.chinatax.gov.cn/zhuanti/ajpgt/article_list.jsp' \
            '?pagenum={}&&city_id={}&tar_type=0'
second_url = 'http://hunan.chinatax.gov.cn/article_listcf.jsp' \
             '?smallclassid=20160216225002&includecity=1&idused=0&cityvisible=1&vid=201001&pagenum={}'
third_url = 'http://hunan.chinatax.gov.cn/zhuanti/qsgg/article_list.jsp?type=1&city_id=-1&pagenum={}'
forth_url = 'http://hunan.chinatax.gov.cn/api.jsp'

gov_name = '湖南省税务局'
collection_name = 'tax_data'

area_list = [
    '20070107095754',
    '20070110012523',
    '20070110012537',
    '20070110012555',
    '20070110012568',
    '20070110012590',
    '20070110012510',
    '20070110012609',
    '20070110012622',
    '20070110012642',
    '20070110012648',
    '20070110012661',
    '20070110012676',
    '20070110012696'
]
params = {
    'theme': '税收征管信息',
    'page': 0
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
    # for each_area in area_list:
    #     url = first_url.format('0', each_area)
    #     response = request_site_page(url)
    #     response.encoding = response.apparent_encoding
    #     stop_flag = False
    #     if response is None:
    #         logger.error('网页请求错误{}'.format(url))
    #     soup = bs(response.content if response else '', 'lxml')
    #
    #     if db.crawler.find({'url': each_area + first_url}).count() > 0:
    #         last_updated_url = db.crawler.find_one({'url': each_area + first_url})['last_updated']
    #     else:
    #         last_updated_url = ''
    #     page_count = int(soup.find('td', style="padding-right:15px;", align="right", colspan="3").find_all('b')[-1].text.strip()) - 1
    #     page_num = 0
    #     while page_num <= page_count:
    #         logger.info('地区：%s, 第%d页' %(each_area, page_num))
    #         try:
    #             data_list = soup.body.find_all('tr')
    #             for index, each_data in enumerate(data_list):
    #                 # 过滤最后一个翻页的tr
    #                 if re.search('查看详情', each_data.text) is None:
    #                     break
    #                 href = each_data.find('a')['href']
    #                 anc_url = urljoin(url, href)
    #
    #                 if anc_url == last_updated_url:
    #                     stop_flag = True
    #                     logger.info('到达上次爬取的链接')
    #                     break
    #                 if index == 0 and page_num == 0:
    #                     if db.crawler.find({'url': each_area + first_url}).count() > 0:
    #                         if db.crawler.find_one({'url': each_area + first_url})['last_updated'] != anc_url:
    #                             db.crawler.update_one({'url': each_area + first_url},
    #                                                   {'$set': {'last_updated': anc_url}})
    #                     else:
    #                         db.crawler.insert_one(
    #                             {'url': each_area + first_url, 'last_updated': anc_url, 'origin': gov_name})
    #                 title = each_data.find('a').text.strip()
    #                 publish_date_raw = re.search('(\d{8})', href).group(1).strip()
    #                 publish_date = publish_date_raw[0: 4] + '-' + publish_date_raw[4: 6] + '-' + publish_date_raw[6: 8]
    #
    #                 if db[collection_name].count_documents({'url': anc_url}) == 0:
    #                     info = {
    #                         'title': title,
    #                         'publishDate': publish_date,
    #                         'url': anc_url,
    #                         'type': '行政处罚决定',
    #                         'origin': gov_name,
    #                         'status': 'not parsed'
    #                     }
    #                     logger.info('{} 新公告：{} url: {}'.format(gov_name, info['title'], anc_url))
    #                     if info not in result_list:
    #                         result_list.append(info)
    #                 else:
    #                     if config['crawler_update_type']['update_type'] == '0':
    #                         break
    #             if stop_flag:
    #                 logger.info('到达上次爬取的链接')
    #                 break
    #             page_num += 1
    #             url = first_url.format(str(page_num), each_area)
    #             response = request_site_page(url)
    #             response.encoding = response.apparent_encoding
    #             soup = bs(response.content if response else '', 'lxml')
    #         except Exception as e:
    #             logger.error(e)
    #             logger.warning('提取公告url出现问题')
    #             continue
    # page_num = 0
    # url = second_url.format(str(page_num))
    # response = request_site_page(url)
    # response.encoding = response.apparent_encoding
    # stop_flag = False
    # if response is None:
    #     logger.error('网页请求错误{}'.format(url))
    # soup = bs(response.content if response else '', 'lxml')
    #
    # if db.crawler.find({'url': second_url}).count() > 0:
    #     last_updated_url = db.crawler.find_one({'url': second_url})['last_updated']
    # else:
    #     last_updated_url = ''
    # page_count_text = soup.find(attrs={"class": "pag"}).find_all('a')[-1]['href']
    # page_count = re.search('pagenum=(\d+)', page_count_text).group(1).strip()
    # while page_num <= int(page_count):
    #     logger.info('second_url, 第%d页' % (page_num))
    #     try:
    #         data_list = soup.find(attrs={"class": "rs_list yc"}).find_all('li')
    #         for index, each_data in enumerate(data_list):
    #             href = each_data.find('a')['href']
    #             anc_url = urljoin(url, href)
    #
    #             if anc_url == last_updated_url:
    #                 stop_flag = True
    #                 logger.info('到达上次爬取的链接')
    #                 break
    #             if index == 0 and page_num == 0:
    #                 if db.crawler.find({'url': second_url}).count() > 0:
    #                     if db.crawler.find_one({'url': second_url})['last_updated'] != anc_url:
    #                         db.crawler.update_one({'url': second_url}, {'$set': {'last_updated': anc_url}})
    #                 else:
    #                     db.crawler.insert_one(
    #                         {'url': second_url, 'last_updated': anc_url, 'origin': gov_name})
    #             title = each_data.find('a')['title']
    #             publish_date = each_data.find('em').text.strip()
    #
    #             if db[collection_name].count_documents({'url': anc_url}) == 0:
    #                 info = {
    #                     'title': title,
    #                     'publishDate': publish_date,
    #                     'url': anc_url,
    #                     'type': '行政处罚决定',
    #                     'origin': gov_name,
    #                     'status': 'not parsed'
    #                 }
    #                 logger.info('{} 新公告：{} url: {}'.format(gov_name, info['title'], anc_url))
    #                 if info not in result_list:
    #                     result_list.append(info)
    #             else:
    #                 if config['crawler_update_type']['update_type'] == '0':
    #                     break
    #         if stop_flag:
    #             logger.info('到达上次爬取的链接')
    #             break
    #         page_num += 1
    #         url = second_url.format(str(page_num))
    #         response = request_site_page(url)
    #         response.encoding = response.apparent_encoding
    #         soup = bs(response.content if response else '', 'lxml')
    #     except Exception as e:
    #         logger.error(e)
    #         logger.warning('提取公告url出现问题')
    #         continue
    # page_num = 0
    # url = third_url.format(str(page_num))
    # response = request_site_page(url)
    # response.encoding = response.apparent_encoding
    # stop_flag = False
    # if response is None:
    #     logger.error('网页请求错误{}'.format(url))
    # soup = bs(response.content if response else '', 'lxml')
    #
    # page_count_text = soup.find_all('a')[-1]['href']
    # page_count = re.search('pagenum=(\d+)', page_count_text).group(1).strip()
    # while page_num <= int(page_count):
    #     logger.info('third_url, 第%d页' % (page_num))
    #     try:
    #         data_list = soup.find_all('table')[-1].find_all('tr')
    #         for each_data in data_list:
    #             if re.search('首页', each_data.text):
    #                 break
    #             all_td = each_data.find_all('td')
    #             address = all_td[0].text + '-' + all_td[5].text + ',纳税人识别号:' + all_td[1].text
    #             litigant = all_td[2].text + ',' + all_td[3].text + ':' + all_td[4].text
    #             facts = all_td[6].text + all_td[7].text + '元'
    #             publish_date = all_td[-1].text.replace('公示时间:', '').strip()
    #
    #             info = {
    #                 'address': address,
    #                 'litigant': litigant,
    #                 'facts': facts,
    #                 'publishDate': publish_date,
    #                 'type': '行政处罚决定',
    #                 'origin': gov_name,
    #                 'status': 'not parsed'
    #             }
    #             if info not in result_list:
    #                 result_list.append(info)
    #         if stop_flag:
    #             logger.info('到达上次爬取的链接')
    #             break
    #         page_num += 1
    #         url = third_url.format(str(page_num))
    #         response = request_site_page(url)
    #         response.encoding = response.apparent_encoding
    #         soup = bs(response.content if response else '', 'lxml')
    #     except Exception as e:
    #         logger.error(e)
    #         logger.warning('提取公告url出现问题')
    #         continue
    page_num = 0
    params['page'] = page_num
    response = request_site_page(forth_url, methods='post', params=params)
    response.encoding = response.apparent_encoding
    stop_flag = False
    if response is None:
        logger.error('网页请求错误{}'.format(forth_url))

    if db.crawler.find({'url': forth_url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': forth_url})['last_updated']
    else:
        last_updated_url = ''
    page_count = response.json()['pagecount']
    base_url = 'http://hunan.chinatax.gov.cn/gkmlcontent.jsp?id='
    while page_num < int(page_count):
        try:
            data_list = response.json()['data']
            for index, each_data in enumerate(data_list):
                title = each_data['title']
                article_id = each_data['id']
                anc_url = base_url + article_id
                if anc_url == last_updated_url:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break
                if index == 0 and page_num == 0:
                    if db.crawler.find({'url': forth_url}).count() > 0:
                        if db.crawler.find_one({'url': forth_url})['last_updated'] != anc_url:
                            db.crawler.update_one({'url': forth_url}, {'$set': {'last_updated': anc_url}})
                    else:
                        db.crawler.insert_one(
                            {'url': forth_url, 'last_updated': anc_url, 'origin': gov_name})
                if re.search('欠税', title):
                    publish_date = each_data['adddate']

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
            params['page'] = page_num
            response = request_site_page(forth_url, methods='post', params=params)
            response.encoding = response.apparent_encoding
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
