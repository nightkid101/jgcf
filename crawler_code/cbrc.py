import re

from pymongo import MongoClient
from bs4 import BeautifulSoup as bs
from init import logger_init, config_init
from utility import request_site_page
from urllib.parse import urljoin

logger = logger_init('中国银监会-数据抓取')
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

# 抓取数据存入cbrc_data这个collection
db.cbrc_data.create_index([('url', 1)])


# 银监会
def cbrc_crawler():
    result_list = []
    prefix_url = ['http://www.cbrc.gov.cn/chinese/home/docViewPage/110002&current=']
    for index, each_url in enumerate(prefix_url):
        # get page_count
        response = request_site_page(each_url)
        if response is None:
            logger.error('网页请求错误 %s' % each_url)
            continue
        soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
        page_count_text = soup.find(id='testUI').text if soup.find(id='testUI') else ''
        page_count = int(re.findall(r'\d+', page_count_text)[-1]) if page_count_text != '' else 0
        logger.info('银监会' + ' 一共有%d页' % page_count)

        # get crawler data
        for num in range(page_count):
            logger.info('银监会 -- 第%d页' % (num + 1))
            url = each_url + str(num + 1)

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
                table_content_list = content_soup.find(id='testUI').find_all('tr') if content_soup.find(id='testUI') else []
                for each_tr in table_content_list:
                    if len(each_tr.find_all('td')) > 1:
                        try:
                            each_link = each_tr.find('a')
                            announcement_url = urljoin(url, each_link.attrs['href'])
                            if db.cbrc_data.find({'url': announcement_url}).count() == 0:
                                title = each_link.attrs['title'].strip()
                                publish_date = each_tr.find_all('td')[-1].text.strip()
                                logger.info('中国银监会新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': publish_date,
                                    'url': announcement_url,
                                    'type': '行政处罚决定',
                                    'origin': '银监会',
                                    'status': 'not parsed'
                                }
                                if post not in result_list:
                                    result_list.append(post)
                            else:
                                if config['crawler_update_type']['update_type'] == '0':
                                    break
                        except Exception as e:
                            logger.error(e)
                            logger.warning('提取公告url出现问题')
                            continue
            except Exception as e:
                logger.error(e)
                continue

    if len(result_list) > 0:
        logger.info('中国银监会一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.cbrc_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('中国银监会公告导入完成！')
        else:
            logger.error('中国银监会导入出现问题！')
    else:
        logger.info('中国银监会没有新公告！')
    logger.info('\n')


# 银监局
def cbrc_crawler_yjj():
    result_list = []
    prefix_url = ['http://www.cbrc.gov.cn/zhuanti/xzcf/get2and3LevelXZCFDocListDividePage//1.html?current=']
    for index, each_url in enumerate(prefix_url):
        # get page_count
        response = request_site_page(each_url)
        if response is None:
            logger.error('网页请求错误 %s' % each_url)
            continue
        soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
        page_count_text = soup.find(id='testUI').text if soup.find(id='testUI') else ''
        page_count = int(re.findall(r'\d+', page_count_text)[-1]) if page_count_text != '' else 0
        logger.info('银监局' + ' 一共有%d页' % page_count)

        # get crawler data
        for num in range(page_count):
            logger.info('银监局 -- 第%d页' % (num + 1))
            url = each_url + str(num + 1)

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
                table_content_list = content_soup.find(id='testUI').find_all('tr') if content_soup.find(id='testUI') else []
                for each_tr in table_content_list:
                    if len(each_tr.find_all('td')) > 1:
                        try:
                            each_link = each_tr.find('a')
                            title = each_link.attrs['title'].strip()
                            publish_date = each_tr.find_all('td')[-1].text.strip()
                            announcement_url = urljoin(url, each_link.attrs['href'])

                            if db.cbrc_data.find(
                                    {
                                        'url':
                                            {'$regex': '.*' + announcement_url.split('/')[-1].replace('.html', '') + '.*'}
                                    }).count() == 0:
                                logger.info('银监局新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': publish_date,
                                    'url': announcement_url,
                                    'type': '行政处罚决定',
                                    'origin': '银监局',
                                    'status': 'not parsed'
                                }
                                if post not in result_list:
                                    result_list.append(post)
                            else:
                                if config['crawler_update_type']['update_type'] == '0':
                                    break
                        except Exception as e:
                            logger.error(e)
                            logger.warning('提取公告url出现问题')
                            continue
            except Exception as e:
                logger.error(e)
                continue

    if len(result_list) > 0:
        logger.info('银监局一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.cbrc_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('银监局公告导入完成！')
        else:
            logger.error('银监局公告导入出现问题！')
    else:
        logger.info('银监局没有新公告！')
    logger.info('\n')


# 行政处罚 银监分局
def cbrc_crawler_yjfj():
    result_list = []
    prefix_url = ['http://www.cbrc.gov.cn/zhuanti/xzcf/get2and3LevelXZCFDocListDividePage//2.html?current=']
    for index, each_url in enumerate(prefix_url):
        # get page_count
        response = request_site_page(each_url)
        if response is None:
            logger.error('网页请求错误 %s' % each_url)
            continue
        soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
        page_count_text = soup.find(id='testUI').text if soup.find(id='testUI') else ''
        page_count = int(re.findall(r'\d+', page_count_text)[-1]) if page_count_text != '' else 0
        logger.info('银监分局' + ' 一共有%d页' % page_count)

        # get crawler data
        for num in range(page_count):
            logger.info('银监分局 -- 第%d页' % (num + 1))
            url = each_url + str(num + 1)

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
                table_content_list = content_soup.find(id='testUI').find_all('tr') if content_soup.find(
                    id='testUI') else []
                for each_tr in table_content_list:
                    if len(each_tr.find_all('td')) > 1:
                        try:
                            each_link = each_tr.find('a')
                            title = each_link.attrs['title'].strip()
                            publish_date = each_tr.find_all('td')[-1].text.strip()
                            announcement_url = urljoin(url, each_link.attrs['href'])

                            if db.cbrc_data.find(
                                    {
                                        'url':
                                            {'$regex': '.*' + announcement_url.split('/')[-1].replace('.html',
                                                                                                      '') + '.*'}
                                    }).count() == 0:
                                logger.info('银监分局新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': publish_date,
                                    'url': announcement_url,
                                    'type': '行政处罚决定',
                                    'origin': '银监分局',
                                    'status': 'not parsed'
                                }
                                if post not in result_list:
                                    result_list.append(post)
                            else:
                                if config['crawler_update_type']['update_type'] == '0':
                                    break
                        except Exception as e:
                            logger.error(e)
                            logger.warning('提取公告url出现问题')
                            continue
            except Exception as e:
                logger.error(e)
                continue

    if len(result_list) > 0:
        logger.info('银监分局一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.cbrc_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('银监分局公告导入完成！')
        else:
            logger.error('银监分局公告导入出现问题！')
    else:
        logger.info('银监分局没有新公告！')
    logger.info('\n')


if __name__ == "__main__":
    cbrc_crawler()
    cbrc_crawler_yjj()
    cbrc_crawler_yjfj()
