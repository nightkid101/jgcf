import re

from pymongo import MongoClient
from utility import request_site_page
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin
from init import logger_init, config_init

logger = logger_init('中国证券业协会-数据抓取')
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

# 抓取数据存入sac_data这个collection
db.sac_data.create_index([('url', 1)])


def sac_crawler():
    prefix_url0 = ['http://www.sac.net.cn/tzgg/']
    prefix_url = ['http://www.sac.net.cn/hyfw/cxjs/gongshi/']
    result_list = []
    for index, each_url in enumerate(prefix_url0):
        # get page count
        response = request_site_page(each_url)
        if response is None:
            logger.error('网页请求错误 %s' % each_url)
            continue
        page_count = int(re.search(r'var countPage = (\d+);', response.text).group(1).strip()) \
            if re.search(r'var countPage = (\d+);', response.text) else 0
        logger.info('中国证券业协会 通知公告' + ' 一共有%d页' % page_count)

        # get crawler data
        for num in range(page_count):
            logger.info('第%d页' % (num + 1))
            if num == 0:
                add_url = 'index.html'
            else:
                add_url = 'index_' + str(num) + '.html'

            try:
                url = each_url + add_url
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
                table_content = content_soup.find(class_='gl_list')

                if not table_content:
                    logger.error('网页请求错误 %s' % url)
                    continue

                for each_tr in table_content.find_all('tr'):
                    try:
                        title = each_tr.find('a').attrs['title'].strip()
                        if re.match('^(关于对).*(采取).*(措施的决定)$', title, flags=0):
                            publish_date = each_tr.find_all('td')[-1].text.strip()
                            announcement_url = urljoin(url, each_tr.find('a').attrs['href'].strip())
                            if db.sac_data.find({'url': announcement_url}).count() == 0:
                                logger.info('中国证券业协会 通知公告 新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': publish_date,
                                    'url': announcement_url,
                                    'type': '监管措施',
                                    'origin': '证券业协会',
                                    'status': 'not parsed'
                                }
                                if post not in result_list:
                                    result_list.append(post)
                            else:
                                if config['crawler_update_type']['update_type'] == '0':
                                    break
                    except Exception as e:
                        logger.error('提取公告url出现问题 %s' % str(e))
                        continue
            except Exception as e:
                logger.error(e)
                continue

    for index, each_url in enumerate(prefix_url):
        # get page count
        response = request_site_page(each_url)
        if response is None:
            logger.error('网页请求错误 %s' % each_url)
            continue
        page_count = int(re.search(r'var countPage = (\d+);', response.text).group(1).strip()) \
            if re.search(r'var countPage = (\d+);', response.text) else 0
        logger.info('中国证券业协会 信息公示' + ' 一共有%d页' % page_count)

        for num in range(page_count):

            logger.info('第%d页' % (num + 1))
            if num == 0:
                add_url = 'index.html'
            else:
                add_url = 'index_' + str(num) + '.html'

            url = each_url + add_url

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
                table_content = content_soup.find(class_='gl_list')

                if not table_content:
                    logger.error('网页请求错误 %s' % url)
                    continue

                for each_tr in table_content.find_all('tr'):
                    try:
                        title = each_tr.find('a').attrs['title'].strip()
                        if re.match('^(关于).*(自律惩戒措施).*$', title, flags=0):
                            announcement_url = urljoin(url, each_tr.find('a').attrs['href'].strip())
                            if db.sac_data.find({'url': announcement_url}).count() == 0:
                                publish_date = each_tr.find_all('td')[-1].text.strip()
                                logger.info('中国证券业协会  信息公示 新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': publish_date,
                                    'url': announcement_url,
                                    'type': '自律惩戒',
                                    'origin': '证券业协会',
                                    'status': 'not parsed'
                                }
                                if post not in result_list:
                                    result_list.append(post)
                            else:
                                if config['crawler_update_type']['update_type'] == '0':
                                    break
                        elif re.match('^(关于).*(从业人员资格考试违纪人员).*(的公告).*$', title, flags=0) or re.match(
                                '^(关于取消).*(请人替考者考试成绩的公告)$', title, flags=0):
                            announcement_url = urljoin(url, each_tr.find('a').attrs['href'].strip())
                            if db.sac_data.find({'url': announcement_url}).count() == 0:
                                publish_date = each_tr.find_all('td')[-1].text.strip()
                                logger.info('中国证券业协会新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': publish_date,
                                    'url': announcement_url,
                                    'type': '监管措施',
                                    'origin': '证券业协会',
                                    'status': 'not parsed'
                                }
                                if post not in result_list:
                                    result_list.append(post)
                            else:
                                if config['crawler_update_type']['update_type'] == '0':
                                    break
                        elif re.match('^(关于对).*(进行纪律处分的决定)$', title, flags=0):
                            announcement_url = urljoin(url, each_tr.find('a').attrs['href'].strip())
                            if db.sac_data.find({'url': announcement_url}).count() == 0:
                                publish_date = each_tr.find_all('td')[-1].text.strip()
                                logger.info('中国证券业协会新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': publish_date,
                                    'url': announcement_url,
                                    'type': '纪律处分',
                                    'origin': '证券业协会',
                                    'status': 'not parsed'
                                }
                                if post not in result_list:
                                    result_list.append(post)
                            else:
                                if config['crawler_update_type']['update_type'] == '0':
                                    break
                        elif re.match('^(关于对).*(进行公开谴责的通知)$', title, flags=0):
                            announcement_url = urljoin(url, each_tr.find('a').attrs['href'].strip())
                            if db.sac_data.find({'url': announcement_url}).count() == 0:
                                publish_date = each_tr.find_all('td')[-1].text.strip()
                                logger.info('中国证券业协会新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': publish_date,
                                    'url': announcement_url,
                                    'type': '公开谴责',
                                    'origin': '证券业协会',
                                    'status': 'not parsed'
                                }
                                if post not in result_list:
                                    result_list.append(post)
                            else:
                                if config['crawler_update_type']['update_type'] == '0':
                                    break
                    except Exception as e:
                        logger.error('提取公告url出现问题 %s' % str(e))
                        continue
            except Exception as e:
                logger.error(e)
                continue

    if len(result_list) > 0:
        logger.info('中国证券业协会一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.sac_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('中国证券业协会公告导入完成！')
        else:
            logger.error('中国证券业协会公告导入出现问题！')
    else:
        logger.info('中国证券业协会没有新公告！')


if __name__ == "__main__":
    sac_crawler()
