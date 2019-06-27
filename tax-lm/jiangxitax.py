import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 江西省税务局
# http://www.jx-n-tax.gov.cn/xxgknew/jcms_files/jcms1/web1/site/col/col2512/index.html
# http://www.jx-n-tax.gov.cn/taxmap/front/result.do
first_url = 'http://www.jx-n-tax.gov.cn/xxgknew/jcms_files/jcms1/web1/site/zfxxgk/search.jsp?showsub=1&orderbysub=2' \
            '&cid=2514&currpage={}&jdid=1&divid=zupei_div&cid='
second_url_base = 'http://www.jx-n-tax.gov.cn/taxmap/front/result.do'
second_url = 'http://www.jx-n-tax.gov.cn/taxmap/front/result2.do?region=&nature=&year=&_=1556954807333&pageno={}'
gov_name = '江西省税务局'
collection_name = 'tax_data'

city_id = ['2512', '2513', '2514', '2515', '2516', '2517', '2518', '2519', '2520', '2521', '2522', '2523']

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
    for each_city in city_id:
        page_num = 1
        url_city = first_url + each_city
        url = url_city.format(str(page_num))
        response = request_site_page(url)
        response.encoding = response.apparent_encoding
        stop_flag = False
        if response is None:
            logger.error('网页请求错误{}'.format(url))
        soup = bs(response.content if response else '', 'lxml')

        if db.crawler.find({'url': each_city + first_url}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': each_city + first_url})['last_updated']
        else:
            last_updated_url = ''
        page_count_text = soup.find(attrs={"class": "tb_title"}).text
        page_count = re.search('共(\d+)页', page_count_text).group(1).strip()
        while page_num <= int(page_count):
            try:
                data_list = soup.find_all(attrs={"class": "tr_main_value_odd"}) + \
                            soup.find_all(attrs={"class": "tr_main_value_even"})
                for index, each_data in enumerate(data_list):
                    anc_url = each_data.find('a')['href']

                    if anc_url == last_updated_url:
                        stop_flag = True
                        logger.info('到达上次爬取的链接')
                        break
                    if index == 0 and page_num == 1:
                        if db.crawler.find({'url': each_city + first_url}).count() > 0:
                            if db.crawler.find_one({'url': each_city + first_url})['last_updated'] != anc_url:
                                db.crawler.update_one({'url': each_city + first_url}, {'$set': {'last_updated': anc_url}})
                        else:
                            db.crawler.insert_one(
                                {'url': each_city + first_url, 'last_updated': anc_url, 'origin': gov_name})

                    title = each_data.find('a')['title']
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
                url = url_city.format(str(page_num))
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
    # page_num = 1
    # response = request_site_page(second_url_base, methods='post')
    # response.encoding = response.apparent_encoding
    # stop_flag = False
    # if response is None:
    #     logger.error('网页请求错误{}'.format(second_url_base))
    # soup = bs(response.content if response else '', 'lxml')
    #
    # if db.crawler.find({'url': second_url_base}).count() > 0:
    #     last_updated_url = db.crawler.find_one({'url': second_url_base})['last_updated']
    # else:
    #     last_updated_url = ''
    # page_count = re.findall('共(\d+)页', soup.text)[-1].strip()
    # while page_num <= int(page_count):
    #     try:
    #         url = second_url.format(str(page_num))
    #         response = request_site_page(url)
    #         response.encoding = response.apparent_encoding
    #         soup = bs(response.content if response else '', 'lxml')
    #         data_list = soup.find_all(attrs={"class": "list3"})
    #         base_url = 'http://www.jx-n-tax.gov.cn/taxmap/front/getdetail.do?iid='
    #         for each_data in data_list:
    #             article_id = re.search('getDetail\((\d+)\)', str(each_data)).group(1).strip()
    #             anc_url = base_url + article_id
    #
    #             if anc_url == last_updated_url:
    #                 stop_flag = True
    #                 logger.info('到达上次爬取的链接')
    #                 break
    #             if db.crawler.find({'url': url}).count() > 0:
    #                 if db.crawler.find_one({'url': url})['last_updated'] != anc_url:
    #                     db.crawler.update_one({'url': url}, {'$set': {'last_updated': anc_url}})
    #             else:
    #                 db.crawler.insert_one(
    #                     {'url': url, 'last_updated': anc_url, 'origin': gov_name})
    #             title = each_data.find(attrs={"class": "list32"}).text.strip()
    #
    #             if db[collection_name].count_documents({'url': anc_url}) == 0:
    #                 info = {
    #                     'title': title,
    #                     'publishDate': '',
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
    #         page_num += 1
    #         if stop_flag:
    #             logger.info('到达上次爬取的链接')
    #             break
    #     except Exception as e:
    #         logger.error(e)
    #         logger.warning('提取公告url出现问题')
    #         continue
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
